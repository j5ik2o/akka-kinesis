package com.github.j5ik2o.ak.kcl.dsl

import akka.stream.scaladsl.Source
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  ShardPrioritization,
  Worker
}
import com.amazonaws.services.kinesis.clientlibrary.types.{ InitializationInput, ShutdownInput }
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.{ RecordProcessorF, WorkerF }
import com.github.j5ik2o.ak.kcl.stage.{ CommittableRecord, KCLSourceStage }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future }

object KCLSource {

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      executionContextExecutorService: Option[ExecutionContextExecutorService] = None,
      kinesisClient: Option[AmazonKinesis] = None,
      dynamoDBClient: Option[AmazonDynamoDB] = None,
      cloudWatchClient: Option[AmazonCloudWatch] = None,
      metricsFactory: Option[IMetricsFactory] = None,
      shardPrioritization: Option[ShardPrioritization] = None,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      recordProcessorF: RecordProcessorF = KCLSourceStage.newDefaultRecordProcessor,
      workerF: WorkerF = KCLSourceStage.newDefaultWorker
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] =
    withoutCheckpoint(
      kinesisClientLibConfiguration,
      executionContextExecutorService,
      kinesisClient,
      dynamoDBClient,
      cloudWatchClient,
      metricsFactory,
      shardPrioritization,
      checkWorkerPeriodicity,
      recordProcessorF,
      workerF
    ).via(KCLFlow.ofCheckpoint())

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      executionContextExecutorService: Option[ExecutionContextExecutorService] = None,
      kinesisClient: Option[AmazonKinesis] = None,
      dynamoDBClient: Option[AmazonDynamoDB] = None,
      cloudWatchClient: Option[AmazonCloudWatch] = None,
      metricsFactory: Option[IMetricsFactory] = None,
      shardPrioritization: Option[ShardPrioritization] = None,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      recordProcessorF: RecordProcessorF = KCLSourceStage.newDefaultRecordProcessor,
      workerF: WorkerF = KCLSourceStage.newDefaultWorker
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] =
    Source.fromGraph(
      new KCLSourceStage(
        kinesisClientLibConfiguration,
        executionContextExecutorService,
        kinesisClient,
        dynamoDBClient,
        cloudWatchClient,
        metricsFactory,
        shardPrioritization,
        checkWorkerPeriodicity,
        recordProcessorF,
        workerF
      )
    )

}
