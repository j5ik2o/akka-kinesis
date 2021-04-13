package com.github.j5ik2o.ak.kpl.stage

import java.util.concurrent.{ CompletableFuture, ExecutionException }

import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.amazonaws.services.kinesis.producer._
import com.github.j5ik2o.ak.kpl.dsl.KPLFlowSettings.{ Exponential, Lineal, RetryBackoffStrategy }

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

class KPLFlowStage(
    kinesisProducerConfiguration: KinesisProducerConfiguration,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit
    ec: ExecutionContext
) extends GraphStageWithMaterializedValue[FlowShape[UserRecord, UserRecordResult], Future[KinesisProducer]] {

  private val in  = Inlet[UserRecord]("KPLFlowStage.int")
  private val out = Outlet[UserRecordResult]("KPLFlowStage.out")

  override def shape: FlowShape[UserRecord, UserRecordResult] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[KinesisProducer]) = {
    val promise = Promise[KinesisProducer]()
    val logic = new TimerGraphStageLogic(shape) with StageLogging {
      type Token      = Int
      type RetryCount = Int

      private case class RequestWithAttempt(request: UserRecord, attempt: Int)

      private case class RequestWithResult(request: UserRecord, result: Try[UserRecordResult], attempt: Int)

      private val retryBaseInMillis                                          = retryInitialTimeout.toMillis
      private val requestWithAttempts: mutable.Queue[RequestWithAttempt]     = mutable.Queue.empty
      private var inFlight: Int                                              = _
      private var completionState: Option[Try[Unit]]                         = _
      private var resultCallback: AsyncCallback[RequestWithResult]           = _
      private val waitingRetries: mutable.HashMap[Token, RequestWithAttempt] = mutable.HashMap.empty
      private var retryToken: Token                                          = _

      private var producer: KinesisProducer = _

      private def tryToExecute(): Unit = {
        log.debug(s"pendingRequests = $requestWithAttempts, isAvailable(out) = ${isAvailable(out)}")
        if (requestWithAttempts.nonEmpty && isAvailable(out)) {
          log.debug("Executing PutRecords call")
          inFlight += 1
          val userRecord = requestWithAttempts.dequeue()
          val jFuture    = producer.addUserRecord(userRecord.request)
          Future { jFuture.get() }
            .transform {
              case Failure(e: ExecutionException) =>
                Failure(e.getCause)
              case x => x
            }.onComplete { triedUserRecordResult =>
              triedUserRecordResult match {
                case Success(userRecordResult) =>
                  push(out, userRecordResult)
                case _ =>
                  ()
              }
              val requestWithResult = RequestWithResult(userRecord.request, triedUserRecordResult, userRecord.attempt)
              resultCallback.invoke(requestWithResult)
            }
        }
      }

      private def checkForCompletion(): Unit =
        if (inFlight == 0 && requestWithAttempts.isEmpty && waitingRetries.isEmpty && isClosed(in)) {
          completionState match {
            case Some(Success(_)) =>
              log.debug("checkForCompletion:completeStage")
              completeStage()
            case Some(Failure(ex)) =>
              log.debug(s"checkForCompletion:failStage($ex)")
              failStage(ex)
            case None =>
              log.debug(s"checkForCompletion:failStage(IllegalStateException)")
              failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      private def handleResult(userRecordResultTry: RequestWithResult): Unit = userRecordResultTry match {
        case RequestWithResult(_, Success(result), _) =>
          log.debug("Get record = {}", result)
          inFlight -= 1
          tryToExecute()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case RequestWithResult(_, Failure(ex: UserRecordFailedException), attempt) if attempt > maxRetries =>
          val last = ex.getResult.getAttempts.asScala.last
          log.error("Record failed to put - {} : {}", last.getErrorCode, last.getErrorMessage)
          failStage(ex)
        case RequestWithResult(error, Failure(ex: UserRecordFailedException), attempt) =>
          log.debug("PutRecords call finished with partial errors; scheduling retry")
          inFlight -= 1
          waitingRetries.put(retryToken, RequestWithAttempt(error, attempt + 1))
          scheduleOnce(
            retryToken,
            backoffStrategy match {
              case Exponential => scala.math.pow(retryBaseInMillis, attempt).toInt millis
              case Lineal      => retryInitialTimeout * attempt
            }
          )
          retryToken += 1
        case RequestWithResult(_, Failure(ex), _) =>
          log.error("Exception during put", ex)
          failStage(ex)
      }

      override def preStart(): Unit = {
        completionState = None
        inFlight = 0
        resultCallback = getAsyncCallback[RequestWithResult](handleResult)
        producer = new KinesisProducer(kinesisProducerConfiguration)
        promise.success(producer)
        pull(in)
      }

      override def postStop(): Unit = {
        log.info("Waiting for remaining puts to finish...")
        producer.flushSync()
        log.info("All records complete.")
        producer.destroy()
        log.info("Finished.")
      }

      override protected def onTimer(timerKey: Any) =
        waitingRetries.remove(timerKey.asInstanceOf[Token]) foreach { requestWithAttempt =>
          log.debug("New PutRecords retry attempt available")
          requestWithAttempts.enqueue(requestWithAttempt)
          tryToExecute()
        }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = {
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush(): Unit = {
            val userRecord = grab(in)
            requestWithAttempts.enqueue(RequestWithAttempt(userRecord, 1))
            tryToExecute()
          }

        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            tryToExecute()
            if (waitingRetries.isEmpty && !hasBeenPulled(in)) tryPull(in)
          }

        }
      )

    }
    (logic, promise.future)
  }

}
