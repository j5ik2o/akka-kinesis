package com.github.j5ik2o.ak.kcl.stage

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.stream.stage.{ AsyncCallback, _ }
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{ IRecordProcessor, IRecordProcessorFactory }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  ShardPrioritization,
  ShutdownReason,
  Worker
}
import com.amazonaws.services.kinesis.clientlibrary.types.{
  ExtendedSequenceNumber,
  InitializationInput,
  ProcessRecordsInput,
  ShutdownInput
}
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.{ KCLMaterializedValue, RecordProcessorF, RecordSet, WorkerF }

import scala.collection.JavaConverters._
import scala.collection.immutable.Queue
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future, Promise }
import scala.util.control.NoStackTrace
import scala.util.{ Failure, Success, Try }

sealed trait KinesisWorkerSourceError extends NoStackTrace

case object WorkerUnexpectedShutdown extends KinesisWorkerSourceError

object KCLSourceStage {

  case class KCLMaterializedValue(workerFuture: Future[Worker], initializationInputFuture: InitializationInput)

  type RecordProcessorF =
    (AsyncCallback[InitializationInput], AsyncCallback[RecordSet], AsyncCallback[ShutdownInput]) => IRecordProcessor

  type WorkerF = (AsyncCallback[InitializationInput], AsyncCallback[RecordSet], AsyncCallback[ShutdownInput]) => Worker

  case class RecordSet(
      recordProcessor: RecordProcessor,
      shardId: String,
      extendedSequenceNumber: ExtendedSequenceNumber,
      cacheEntryTime: Instant,
      cacheExitTIme: Instant,
      timeSpentInCache: FiniteDuration,
      records: Seq[Record],
      millisBehindLatest: Long,
      recordProcessorCheckPointer: IRecordProcessorCheckpointer
  ) {

    def checkPoint: Try[Unit] =
      if (recordProcessor.maybeShutdownReason.nonEmpty) Success(())
      else Try { recordProcessorCheckPointer.checkpoint(records.last) }
  }

  def newDefaultRecordProcessor: RecordProcessorF =
    (onInitializeCallback, onRecordsCallback, onShutdownCallback) =>
      new RecordProcessor(onInitializeCallback, onRecordsCallback, onShutdownCallback)

  def newRecordProcessorFactory(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onRecordCallback: AsyncCallback[RecordSet],
      onShutdownCallback: AsyncCallback[ShutdownInput]
  ): IRecordProcessorFactory =
    () => newDefaultRecordProcessor(onInitializeCallback, onRecordCallback, onShutdownCallback)

  def newDefaultWorker(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory],
      executionContextExecutorService: Option[ExecutionContextExecutorService],
      amazonKinesisOpt: Option[AmazonKinesis],
      amazonDynamoDBOpt: Option[AmazonDynamoDB],
      amazonCloudWatchOpt: Option[AmazonCloudWatch],
      iMetricsFactoryOpt: Option[IMetricsFactory],
      shardPrioritizationOpt: Option[ShardPrioritization]
  ): WorkerF = {
    (
        onInitializeCallback: AsyncCallback[InitializationInput],
        onRecordCallback: AsyncCallback[RecordSet],
        onShutdownCallback: AsyncCallback[ShutdownInput]
    ) =>
      {
        new Worker.Builder()
          .recordProcessorFactory(
            recordProcessorFactoryOpt.getOrElse(
              newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
            )
          )
          .config(kinesisClientLibConfiguration)
          .execService(executionContextExecutorService.orNull)
          .kinesisClient(amazonKinesisOpt.orNull)
          .dynamoDBClient(amazonDynamoDBOpt.orNull)
          .cloudWatchClient(amazonCloudWatchOpt.orNull)
          .metricsFactory(iMetricsFactoryOpt.orNull)
          .shardPrioritization(shardPrioritizationOpt.orNull)
          .build()
      }
  }

  class RecordProcessor(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onRecordsCallback: AsyncCallback[RecordSet],
      onShutdownCallback: AsyncCallback[ShutdownInput]
  ) extends IRecordProcessor {
    private[this] var _shardId: String                                         = _
    private[this] var _extendedSequenceNumber: ExtendedSequenceNumber          = _
    private[this] var _pendingCheckpointSequenceNumber: ExtendedSequenceNumber = _
    private[this] var _maybeShutdownReason: Option[ShutdownReason]             = None

    def shardId: String                                         = _shardId
    def extendedSequenceNumber: ExtendedSequenceNumber          = _extendedSequenceNumber
    def pendingCheckpointSequenceNumber: ExtendedSequenceNumber = _pendingCheckpointSequenceNumber
    def maybeShutdownReason: Option[ShutdownReason]             = _maybeShutdownReason

    override def initialize(initializationInput: InitializationInput): Unit = {
      _shardId = initializationInput.getShardId
      _extendedSequenceNumber = initializationInput.getExtendedSequenceNumber
      _pendingCheckpointSequenceNumber = initializationInput.getPendingCheckpointSequenceNumber
      onInitializeCallback.invoke(initializationInput)
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val cacheEntryTime     = processRecordsInput.getCacheEntryTime
      val cacheExitTIme      = processRecordsInput.getCacheExitTime
      val timeSpentInCache   = FiniteDuration(processRecordsInput.getTimeSpentInCache.toNanos, TimeUnit.NANOSECONDS)
      val records            = processRecordsInput.getRecords.asScala
      val millisBehindLatest = processRecordsInput.getMillisBehindLatest
      val checkPointer       = processRecordsInput.getCheckpointer
      val recordSet =
        RecordSet(
          this,
          _shardId,
          _extendedSequenceNumber,
          cacheEntryTime,
          cacheExitTIme,
          timeSpentInCache,
          records.toVector,
          millisBehindLatest,
          checkPointer
        )
      onRecordsCallback.invoke(recordSet)
    }

    override def shutdown(shutdownInput: ShutdownInput): Unit = {
      _maybeShutdownReason = Some(shutdownInput.getShutdownReason)
      onShutdownCallback.invoke(shutdownInput)
    }
  }
}

/*

    kinesisClientLibConfiguration: KinesisClientLibConfiguration,
    executionContextExecutorService: Option[ExecutionContextExecutorService] = None,
    kinesisClient: Option[AmazonKinesis] = None,
    dynamoDBClient: Option[AmazonDynamoDB] = None,
    cloudWatchClient: Option[AmazonCloudWatch] = None,
    metricsFactory: Option[IMetricsFactory] = None,
    shardPrioritization: Option[ShardPrioritization] = None,
    checkWorkerPeriodicity: FiniteDuration = 1 seconds,
 */
class KCLSourceStage(
    checkWorkerPeriodicity: FiniteDuration = 1 seconds,
    workerF: WorkerF
)(implicit ec: ExecutionContext)
    extends GraphStageWithMaterializedValue[SourceShape[CommittableRecord], Future[Worker]] {

  private val out: Outlet[CommittableRecord] = Outlet("KCLSource.out")

  override def shape: SourceShape[CommittableRecord] = SourceShape(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Worker]) = {
    val workerPromise = Promise[Worker]()
    val logic = new TimerGraphStageLogic(shape) with StageLogging {

      private var worker: Worker = _

      private var buffer: Queue[RecordSet] = Queue.empty[RecordSet]

      private val onInitializeCallback: AsyncCallback[InitializationInput] = getAsyncCallback { initializationInput =>
        log.info(
          s"shardId = ${initializationInput.getShardId}, extendedSequenceNumber = ${initializationInput.getExtendedSequenceNumber}, pendingCheckpointSequenceNumber = ${initializationInput.getPendingCheckpointSequenceNumber}"
        )
      }

      private val onRecordSetCallback: AsyncCallback[RecordSet] = getAsyncCallback { recordSet =>
        buffer = buffer.enqueue(recordSet)
        tryToProduce()
      }

      private val onShutdownCallback: AsyncCallback[ShutdownInput] = getAsyncCallback { shutdownInput =>
        log.debug("shutdownInput.getShutdownReason = {}", shutdownInput.getShutdownReason)
        if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
          Try {
            shutdownInput.getCheckpointer.checkpoint()
          } match {
            case Success(_) =>
              log.debug("when shutdown, checkpoint is success!")
            case Failure(ex: Throwable) =>
              log.error("when shutdown, checkpoint is failure!!!", ex)
              fail(out, ex)
          }
        }
      }

      private def tryToProduce(): Unit =
        if (buffer.nonEmpty && isAvailable(out)) {
          val (head, tail) = buffer.dequeue
          buffer = tail
          emitMultiple(
            out,
            head.records.map { v =>
              new CommittableRecord(
                shardId = head.shardId,
                recordProcessorStartingSequenceNumber = head.extendedSequenceNumber,
                millisBehindLatest = head.millisBehindLatest,
                v,
                head.recordProcessor,
                head.recordProcessorCheckPointer
              )
            }.toList
          )
        }

      override protected def onTimer(timerKey: Any): Unit = {
        if (worker.hasGracefulShutdownStarted && isAvailable(out)) {
          failStage(WorkerUnexpectedShutdown)
        }
      }

      override def preStart(): Unit = {
        try {
          worker = workerF(onInitializeCallback, onRecordSetCallback, onShutdownCallback)
          log.info(s"Created Worker instance {} of application {}", worker, worker.getApplicationName)
          scheduleAtFixedRate("check-worker-shutdown", checkWorkerPeriodicity, checkWorkerPeriodicity)
          ec.execute(worker)
          workerPromise.success(worker)
        } catch {
          case ex: Exception =>
            workerPromise.failure(ex)
            throw ex
        }
      }

      override def postStop(): Unit = {
        buffer = Queue.empty[RecordSet]
        worker.shutdown()
        log.info(s"Shut down Worker instance {} of application {}", worker, worker.getApplicationName)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = tryToProduce()
      })

    }
    (logic, workerPromise.future)
  }
}
