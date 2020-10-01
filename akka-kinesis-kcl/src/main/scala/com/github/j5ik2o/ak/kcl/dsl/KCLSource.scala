package com.github.j5ik2o.ak.kcl.dsl

import akka.stream.scaladsl.Source
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  ShardPrioritization,
  Worker
}
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.WorkerF
import com.github.j5ik2o.ak.kcl.stage.{ CommittableRecord, KCLSourceStage }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future }

object KCLSource {

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      amazonKinesisOpt: Option[AmazonKinesis] = None,
      amazonDynamoDBOpt: Option[AmazonDynamoDB] = None,
      amazonCloudWatchOpt: Option[AmazonCloudWatch] = None,
      iMetricsFactoryOpt: Option[IMetricsFactory] = None,
      shardPrioritizationOpt: Option[ShardPrioritization] = None,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None,
      executionContextExecutorService: Option[ExecutionContextExecutorService] = None
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] = {
    withCheckpointWithWorker(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        recordProcessorFactoryOpt,
        executionContextExecutorService,
        amazonKinesisOpt,
        amazonDynamoDBOpt,
        amazonCloudWatchOpt,
        iMetricsFactoryOpt,
        shardPrioritizationOpt
      )
    )
  }

  def withCheckpointWithWorker(
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      workerF: WorkerF
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] =
    withoutCheckpointWithWorker(
      checkWorkerPeriodicity,
      workerF
    ).via(KCLFlow.ofCheckpoint())

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      amazonKinesisOpt: Option[AmazonKinesis],
      amazonDynamoDBOpt: Option[AmazonDynamoDB],
      amazonCloudWatchOpt: Option[AmazonCloudWatch],
      iMetricsFactoryOpt: Option[IMetricsFactory],
      shardPrioritizationOpt: Option[ShardPrioritization],
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory],
      executionContextExecutorService: Option[ExecutionContextExecutorService]
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] = {
    withoutCheckpointWithWorker(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        recordProcessorFactoryOpt,
        executionContextExecutorService,
        amazonKinesisOpt,
        amazonDynamoDBOpt,
        amazonCloudWatchOpt,
        iMetricsFactoryOpt,
        shardPrioritizationOpt
      )
    )
  }

  def withoutCheckpointWithWorker(
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      workerF: WorkerF
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] =
    Source.fromGraph(new KCLSourceStage(checkWorkerPeriodicity, workerF))

}
