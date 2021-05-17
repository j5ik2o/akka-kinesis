package com.github.j5ik2o.ak.kcl.v2.stage

import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.github.j5ik2o.ak.kcl.v2.stage.KCLSourceStage.SchedulerF
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.{ CoordinatorConfig, Scheduler }
import software.amazon.kinesis.exceptions.{ InvalidStateException, ShutdownException }
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.lifecycle.LifecycleConfig
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.processor.{
  ProcessorConfig,
  RecordProcessorCheckpointer,
  ShardRecordProcessor,
  ShardRecordProcessorFactory
}
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber
import software.amazon.kinesis.retrieval.{ KinesisClientRecord, RetrievalConfig }

import java.nio.ByteBuffer
import scala.collection.immutable.Queue
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace

sealed trait KinesisSchedulerSourceError extends NoStackTrace

case object SchedulerUnexpectedShutdown extends KinesisSchedulerSourceError

final class CommittableRecord(
    val millisBehindLatest: Long,
    val record: KinesisClientRecord,
    val recordProcessorCheckpointer: RecordProcessorCheckpointer
) {
  val partitionKey: String    = record.partitionKey()
  val sequenceNumber: String  = record.sequenceNumber()
  val explicitHashKey: String = record.explicitHashKey()
  val data: ByteBuffer        = record.data()

  def checkpoint()(implicit executor: ExecutionContext): Future[Unit] =
    Future(recordProcessorCheckpointer.checkpoint(record.sequenceNumber(), record.subSequenceNumber()))
}

object KCLSourceStage {

  val logger = LoggerFactory.getLogger(getClass)

  type SchedulerF = (
      AsyncCallback[InitializationInput],
      AsyncCallback[ProcessRecordsInput],
      AsyncCallback[LeaseLostInput],
      AsyncCallback[ShardEndedInput],
      AsyncCallback[ShutdownRequestedInput]
  ) => Scheduler

  type RecordProcessorF =
    (
        AsyncCallback[InitializationInput],
        AsyncCallback[ProcessRecordsInput],
        AsyncCallback[LeaseLostInput],
        AsyncCallback[ShardEndedInput],
        AsyncCallback[ShutdownRequestedInput]
    ) => ShardRecordProcessor

  def newDefaultRecordProcessor: RecordProcessorF =
    (
        onInitializeCallback,
        onProcessRecordsInputCallback,
        onLeaseLostInputCallback,
        onShardEndedInputCallback,
        onShutdownRequestedInputCallback
    ) => {
      logger.debug("newDefaultRecordProcessor -- start")
      val result = new MyRecordProcessor(
        onInitializeCallback,
        onProcessRecordsInputCallback,
        onLeaseLostInputCallback,
        onShardEndedInputCallback,
        onShutdownRequestedInputCallback
      )
      logger.debug("newDefaultRecordProcessor -- finish")
      result
    }

  def newShardRecordProcessorFactory(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onProcessRecordsInputCallback: AsyncCallback[ProcessRecordsInput],
      onLeaseLostInputCallback: AsyncCallback[LeaseLostInput],
      onShardEndedInputCallback: AsyncCallback[ShardEndedInput],
      onShutdownRequestedInputCallback: AsyncCallback[ShutdownRequestedInput]
  ): ShardRecordProcessorFactory = new ShardRecordProcessorFactory {
    logger.debug("newShardRecordProcessorFactory -- construct")

    override def shardRecordProcessor(): ShardRecordProcessor = {
      logger.debug("shardRecordProcessor -- start")
      val result = newDefaultRecordProcessor(
        onInitializeCallback,
        onProcessRecordsInputCallback,
        onLeaseLostInputCallback,
        onShardEndedInputCallback,
        onShutdownRequestedInputCallback
      )
      logger.debug("shardRecordProcessor -- finish")
      result
    }
  }

  final class MyRecordProcessor(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onProcessRecordsInputCallback: AsyncCallback[ProcessRecordsInput],
      onLeaseLostInputCallback: AsyncCallback[LeaseLostInput],
      onShardEndedInputCallback: AsyncCallback[ShardEndedInput],
      onShutdownRequestedInputCallback: AsyncCallback[ShutdownRequestedInput]
  ) extends ShardRecordProcessor {
    private val logger = LoggerFactory.getLogger(getClass)

    private[this] var _shardId: String                                         = _
    private[this] var _extendedSequenceNumber: ExtendedSequenceNumber          = _
    private[this] var _pendingCheckpointSequenceNumber: ExtendedSequenceNumber = _

    def shardId: String                                         = _shardId
    def extendedSequenceNumber: ExtendedSequenceNumber          = _extendedSequenceNumber
    def pendingCheckpointSequenceNumber: ExtendedSequenceNumber = _pendingCheckpointSequenceNumber

    override def initialize(initializationInput: InitializationInput): Unit = {
      _shardId = initializationInput.shardId()
      _extendedSequenceNumber = initializationInput.extendedSequenceNumber()
      _pendingCheckpointSequenceNumber = initializationInput.pendingCheckpointSequenceNumber()
      onInitializeCallback.invoke(initializationInput)
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      onProcessRecordsInputCallback.invoke(processRecordsInput)
    }

    override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
      onLeaseLostInputCallback.invoke(leaseLostInput)
    }

    override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
      try {
        logger.info("Reached shard end checkpointing.")
        shardEndedInput.checkpointer().checkpoint()
        onShardEndedInputCallback.invoke(shardEndedInput)
      } catch {
        case ex: ShutdownException =>
          logger.error("Exception while checkpointing at shard end. Giving up.", ex)
        case ex: InvalidStateException =>
          logger.error("Exception while checkpointing at shard end. Giving up.", ex)
      }
    }

    override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
      try {
        logger.info("Scheduler is shutting down, checkpointing.")
        shutdownRequestedInput.checkpointer().checkpoint()
        onShutdownRequestedInputCallback.invoke(shutdownRequestedInput)
      } catch {
        case ex: ShutdownException =>
          logger.error("Exception while checkpointing at requested shutdown. Giving up.", ex)
        case ex: InvalidStateException =>
          logger.error("Exception while checkpointing at requested shutdown. Giving up.", ex)
      }
    }
  }

  def newConfigBuilderF(
      streamName: String,
      applicationName: String,
      schedulerIdentifier: String,
      kinesisClient: KinesisAsyncClient,
      dynamodbClient: DynamoDbAsyncClient,
      cloudWatchClient: CloudWatchAsyncClient,
      recordProcessorFactoryOpt: Option[ShardRecordProcessorFactory]
  ): (
      AsyncCallback[InitializationInput],
      AsyncCallback[ProcessRecordsInput],
      AsyncCallback[LeaseLostInput],
      AsyncCallback[ShardEndedInput],
      AsyncCallback[ShutdownRequestedInput]
  ) => ConfigsBuilder = {
    (
        onInitializeCallback: AsyncCallback[InitializationInput],
        onProcessRecordsInputCallback: AsyncCallback[ProcessRecordsInput],
        onLeaseLostInputCallback: AsyncCallback[LeaseLostInput],
        onShardEndedInputCallback: AsyncCallback[ShardEndedInput],
        onShutdownRequestedInputCallback: AsyncCallback[ShutdownRequestedInput]
    ) =>
      new ConfigsBuilder(
        streamName,
        applicationName,
        kinesisClient,
        dynamodbClient,
        cloudWatchClient,
        schedulerIdentifier,
        recordProcessorFactoryOpt.getOrElse(
          newShardRecordProcessorFactory(
            onInitializeCallback,
            onProcessRecordsInputCallback,
            onLeaseLostInputCallback,
            onShardEndedInputCallback,
            onShutdownRequestedInputCallback
          )
        )
      )

  }

  def newScheduler(
      checkpointConfig: CheckpointConfig,
      coordinatorConfig: CoordinatorConfig,
      leaseManagementConfig: LeaseManagementConfig,
      lifecycleConfig: LifecycleConfig,
      metricsConfig: MetricsConfig,
      processorConfig: ProcessorConfig,
      retrievalConfig: RetrievalConfig
  ): Scheduler = {
    new Scheduler(
      checkpointConfig,
      coordinatorConfig,
      leaseManagementConfig,
      lifecycleConfig,
      metricsConfig,
      processorConfig,
      retrievalConfig
    )
  }

}

class KCLSourceStage(
    checkSchedulerPeriodicity: FiniteDuration = 1.seconds,
    schedulerF: SchedulerF
)(implicit ec: ExecutionContext)
    extends GraphStageWithMaterializedValue[SourceShape[CommittableRecord], Future[Scheduler]] {

  private val out: Outlet[CommittableRecord] = Outlet("KCLSourceV2.out")

  override def shape: SourceShape[CommittableRecord] = SourceShape(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Scheduler]) = {
    val schedulerPromise = Promise[Scheduler]()
    val logic = new TimerGraphStageLogic(shape) with StageLogging {
      private var scheduler: Scheduler               = _
      private var buffer: Queue[ProcessRecordsInput] = Queue.empty[ProcessRecordsInput]

      private val onInitializeCallback: AsyncCallback[InitializationInput] = getAsyncCallback { initializationInput =>
        log.debug(s"onInitializeCallback: initializationInput = $initializationInput")
      }

      private val onProcessRecordsInputCallback: AsyncCallback[ProcessRecordsInput] = getAsyncCallback {
        processRecordsInput =>
          log.debug(s"onRecordSetCallback: processRecordsInput = $processRecordsInput")
          buffer = buffer.enqueue(processRecordsInput)
          tryToProduce()
      }

      private val onLeaseLostInputCallback: AsyncCallback[LeaseLostInput] = getAsyncCallback { leaseLostInput =>
        log.debug(s"onLeaseLostInputCallback: leaseLostInput = $leaseLostInput")
      }

      private val onShardEndedInputCallback: AsyncCallback[ShardEndedInput] = getAsyncCallback { shardEndedInput =>
        log.debug(s"onShardEndedInputCallback: shardEndedInput = $shardEndedInput")
      }

      private val onShutdownRequestedInputCallback: AsyncCallback[ShutdownRequestedInput] = getAsyncCallback {
        shutdownRequestedInput =>
          log.debug(s"onShutdownRequestedInputCallback: shutdownRequestedInput = $shutdownRequestedInput")
      }

      private def tryToProduce(): Unit = {
        log.debug("tryToProduce -- start")
        if (buffer.nonEmpty && isAvailable(out)) {
          val (head, tail) = buffer.dequeue
          val records = head
            .records().asScala.map { record: KinesisClientRecord =>
              val checkpointer: RecordProcessorCheckpointer = head.checkpointer()
              val millisBehindLatest                        = head.millisBehindLatest()
              new CommittableRecord(
                millisBehindLatest,
                record,
                checkpointer
              )
            }.toVector
          buffer = tail
          emitMultiple(out, records)
          log.debug(s"tryToProduce: emitMultiple: records = $head")
        }
        log.debug("tryToProduce -- finish")
      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case "check-scheduler-shutdown" =>
            if (scheduler.hasGracefulShutdownStarted && isAvailable(out)) {
              log.warning(s"onTimer($timerKey): failStage: scheduler unexpected shutdown")
              failStage(SchedulerUnexpectedShutdown)
            }
          case _ =>
            throw new IllegalStateException(s"Invalid timerKey: timerKey = ${timerKey.toString}")
        }
      }

      override def preStart(): Unit = {
        try {
          log.debug("preStart -- start")
          scheduler = schedulerF(
            onInitializeCallback,
            onProcessRecordsInputCallback,
            onLeaseLostInputCallback,
            onShardEndedInputCallback,
            onShutdownRequestedInputCallback
          )
          log.info(s"Created Scheduler instance: scheduler = ${scheduler.applicationName}")
          scheduleAtFixedRate("check-scheduler-shutdown", checkSchedulerPeriodicity, checkSchedulerPeriodicity)
          val th = new Thread(scheduler)
          th.setDaemon(true);
          th.start()
          schedulerPromise.success(scheduler)
        } catch {
          case ex: Throwable =>
            schedulerPromise.failure(ex)
        } finally {
          log.debug("preStart -- finish")
        }
      }

      override def postStop(): Unit = {
        buffer = Queue.empty[ProcessRecordsInput]
        scheduler.startGracefulShutdown().get()
        log.info(s"Shut down scheduler instance: scheduler = ${scheduler.applicationName()}")
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = tryToProduce()
      })
    }
    (logic, schedulerPromise.future)
  }
}
