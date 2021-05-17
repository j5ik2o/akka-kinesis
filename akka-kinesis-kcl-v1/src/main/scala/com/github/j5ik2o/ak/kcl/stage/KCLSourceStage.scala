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
  LeaderDecider,
  ShardPrioritization,
  ShardSyncer,
  ShutdownReason,
  Worker,
  WorkerStateChangeListener
}
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy
import com.amazonaws.services.kinesis.clientlibrary.types.{
  ExtendedSequenceNumber,
  InitializationInput,
  ProcessRecordsInput,
  ShutdownInput
}
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease
import com.amazonaws.services.kinesis.leases.interfaces.{ ILeaseManager, ILeaseRenewer, ILeaseTaker, LeaseSelector }
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.{ RecordSet, WorkerF }

import scala.collection.JavaConverters._
import scala.collection.immutable.Queue
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future, Promise }
import scala.util.control.{ NoStackTrace, NonFatal }
import scala.util.{ Success, Try }

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
      records: Vector[Record],
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
      amazonKinesisOpt: Option[AmazonKinesis],
      amazonDynamoDBOpt: Option[AmazonDynamoDB],
      amazonCloudWatchOpt: Option[AmazonCloudWatch],
      iMetricsFactoryOpt: Option[IMetricsFactory],
      leaseManager: Option[ILeaseManager[KinesisClientLease]],
      executionContextExecutorService: Option[ExecutionContextExecutorService],
      shardPrioritizationOpt: Option[ShardPrioritization],
      kinesisProxy: Option[IKinesisProxy],
      workerStateChangeListener: Option[WorkerStateChangeListener],
      leaseSelector: Option[LeaseSelector[KinesisClientLease]],
      leaderDecider: Option[LeaderDecider],
      leaseTaker: Option[ILeaseTaker[KinesisClientLease]],
      leaseRenewer: Option[ILeaseRenewer[KinesisClientLease]],
      shardSyncer: Option[ShardSyncer],
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory]
  ): WorkerF = {
    (
        onInitializeCallback: AsyncCallback[InitializationInput],
        onRecordCallback: AsyncCallback[RecordSet],
        onShutdownCallback: AsyncCallback[ShutdownInput]
    ) =>
      {
        new Worker.Builder()
          .config(kinesisClientLibConfiguration)
          .kinesisClient(amazonKinesisOpt.orNull)
          .dynamoDBClient(amazonDynamoDBOpt.orNull)
          .cloudWatchClient(amazonCloudWatchOpt.orNull)
          .metricsFactory(iMetricsFactoryOpt.orNull)
          .leaseManager(leaseManager.orNull)
          .execService(executionContextExecutorService.orNull)
          .shardPrioritization(shardPrioritizationOpt.orNull)
          .kinesisProxy(kinesisProxy.orNull)
          .workerStateChangeListener(workerStateChangeListener.orNull)
          .leaseSelector(leaseSelector.orNull)
          .leaderDecider(leaderDecider.orNull)
          .leaseTaker(leaseTaker.orNull)
          .leaseRenewer(leaseRenewer.orNull)
          .shardSyncer(shardSyncer.orNull)
          .recordProcessorFactory(
            recordProcessorFactoryOpt.getOrElse(
              newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
            )
          )
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

class KCLSourceStage(
    checkWorkerPeriodicity: FiniteDuration = 1.seconds,
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
        log.debug(s"onInitializeCallback: initializationInput = $initializationInput")
      }

      private val onRecordSetCallback: AsyncCallback[RecordSet] = getAsyncCallback { recordSet =>
        log.debug(s"onRecordSetCallback: recordSet = $recordSet")
        buffer = buffer.enqueue(recordSet)
        tryToProduce()
      }

      private val onShutdownCallback: AsyncCallback[ShutdownInput] = getAsyncCallback { shutdownInput =>
        log.debug(s"onShutdownCallback: shutdownInput = $shutdownInput")
        if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
          try {
            shutdownInput.getCheckpointer.checkpoint()
            log.debug("onShutdownCallback: checkpoint is success!")
          } catch {
            case NonFatal(ex: Throwable) =>
              log.error("onShutdownCallback: checkpoint is failure!!!", ex)
              fail(out, ex)
          }
        }
      }

      private def tryToProduce(): Unit =
        if (buffer.nonEmpty && isAvailable(out)) {
          val (head, tail) = buffer.dequeue
          buffer = tail
          val records = head.records.map { record: Record =>
            new CommittableRecord(
              shardId = head.shardId,
              recordProcessorStartingSequenceNumber = head.extendedSequenceNumber,
              millisBehindLatest = head.millisBehindLatest,
              record,
              head.recordProcessor,
              head.recordProcessorCheckPointer
            )
          }
          emitMultiple(out, records)
          log.debug(s"tryToProduce: emitMultiple: records = $records")
        }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case "check-worker-shutdown" =>
            if (worker.hasGracefulShutdownStarted && isAvailable(out)) {
              log.warning(s"onTimer($timerKey): failStage: worker unexpected shutdown")
              failStage(WorkerUnexpectedShutdown)
            }
          case _ =>
            throw new IllegalStateException(s"Invalid timerKey: timerKey = ${timerKey.toString}")
        }
      }

      override def preStart(): Unit = {
        try {
          worker = workerF(onInitializeCallback, onRecordSetCallback, onShutdownCallback)
          log.info(s"Created Worker instance: worker = ${worker.getApplicationName}")
          scheduleAtFixedRate("check-worker-shutdown", checkWorkerPeriodicity, checkWorkerPeriodicity)
          ec.execute(worker)
          workerPromise.success(worker)
        } catch {
          case NonFatal(ex) =>
            workerPromise.failure(ex)
            throw ex
        }
      }

      override def postStop(): Unit = {
        buffer = Queue.empty[RecordSet]
        worker.startGracefulShutdown().get()
        log.info(s"Shut down Worker instance: worker = ${worker.getApplicationName}")
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = tryToProduce()
      })

    }
    (logic, workerPromise.future)
  }
}
