package com.github.j5ik2o.ak.kpl

import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.amazonaws.services.kinesis.producer._
import com.github.j5ik2o.ak.JavaFutureConverter._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class KPLFlowStage(kinesisProducerConfiguration: KinesisProducerConfiguration, settings: KPLFLowSettings)(
    implicit ec: ExecutionContext
) extends GraphStage[FlowShape[UserRecord, UserRecordResult]] {
  private val in: Inlet[UserRecord]         = Inlet("KPLFlow.int")
  private val out: Outlet[UserRecordResult] = Outlet("KPLFlow.out")

  override def shape: FlowShape[UserRecord, UserRecordResult] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {
      type Token      = Int
      type RetryCount = Int

      private case class RequestWithAttempt(userRecord: UserRecord, attempt: Int = 1)
      private case class RequestWithResult(request: UserRecord, result: Try[UserRecordResult], attempt: Int)

      private val retryBaseInMillis                                          = settings.retryInitialTimeout.toMillis
      private var inFlight: Int                                              = _
      private var completionState: Option[Try[Unit]]                         = _
      private val requestWithAttempts: mutable.Queue[RequestWithAttempt]     = mutable.Queue.empty
      private var resultCallback: AsyncCallback[RequestWithResult]           = _
      private val waitingRetries: mutable.HashMap[Token, RequestWithAttempt] = mutable.HashMap.empty
      private var retryToken: Token                                          = _
      private var producer: KinesisProducer                                  = _

      private def tryToProduce(): Unit = {
        if (requestWithAttempts.nonEmpty && isAvailable(out)) {
          val requestWithAttempt = requestWithAttempts.dequeue()
          producer.addUserRecord(requestWithAttempt.userRecord).toScala.onComplete {
            case r @ Success(userRecordResult) =>
              push(out, userRecordResult)
              val requestWithResult = RequestWithResult(requestWithAttempt.userRecord, r, requestWithAttempt.attempt)
              resultCallback.invoke(requestWithResult)
            case r =>
              val requestWithResult = RequestWithResult(requestWithAttempt.userRecord, r, requestWithAttempt.attempt)
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
          tryToProduce()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case RequestWithResult(_, Failure(ex: UserRecordFailedException), attempt) if attempt > settings.maxRetries =>
          val last = ex.getResult.getAttempts.asScala.last
          log.error("Record failed to put - {} : {}", last.getErrorCode, last.getErrorMessage)
          failStage(ex)
        case RequestWithResult(error, Failure(ex: UserRecordFailedException), attempt) =>
          log.debug("PutRecords call finished with partial errors; scheduling retry")
          inFlight -= 1
          waitingRetries.put(retryToken, RequestWithAttempt(error, attempt + 1))
          scheduleOnce(
            retryToken,
            settings.backoffStrategy match {
              case RetryBackoffStrategy.Exponential => scala.math.pow(retryBaseInMillis, attempt).toInt millis
              case RetryBackoffStrategy.Lineal      => settings.retryInitialTimeout * attempt
            }
          )
          retryToken += 1
        case RequestWithResult(_, Failure(ex), _) =>
          log.error("Exception during put", ex)
          failStage(ex)
      }

      override protected def onTimer(timerKey: Any) =
        waitingRetries.remove(timerKey.asInstanceOf[Token]) foreach { requestWithAttempt =>
          log.debug("New PutRecords retry attempt available")
          requestWithAttempts.enqueue(requestWithAttempt)
          tryToProduce()
        }

      override def preStart(): Unit = {
        completionState = None
        inFlight = 0
        resultCallback = getAsyncCallback[RequestWithResult](handleResult)
        producer = new KinesisProducer(kinesisProducerConfiguration)
        pull(in)
      }

      override def postStop(): Unit = {
        log.info("Waiting for remaining puts to finish...")
        producer.flushSync()
        log.info("All records complete.")
        producer.destroy()
        log.info("Finished.")
      }

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
        requestWithAttempts.enqueue(RequestWithAttempt(userRecord))
        tryToProduce()
      }

      override def onPull(): Unit = {
        tryToProduce()
        if (waitingRetries.isEmpty && !hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)
    }

}
