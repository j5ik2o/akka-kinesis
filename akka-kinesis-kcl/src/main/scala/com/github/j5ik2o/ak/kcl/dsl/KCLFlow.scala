package com.github.j5ik2o.ak.kcl.dsl

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Source }
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, ShardPrioritization }
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.{ CommittableRecord, KCLSourceStage }
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.{ RecordProcessor, RecordProcessorF }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future }

object KCLFlow {

  def ofCheckpoint()(implicit ec: ExecutionContext): Flow[CommittableRecord, Record, NotUsed] =
    Flow[CommittableRecord]
      .mapAsync(1) { v =>
        if (v.canBeCheckpointed()) {
          v.checkpoint().map { _ =>
            v
          }
        } else Future.successful(v)
      }
      .map(_.record)

}

object KCLSource {

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      recordProcessorF: RecordProcessorF = RecordProcessor,
      executionContextExecutorService: Option[ExecutionContextExecutorService] = None,
      kinesisClient: Option[AmazonKinesis] = None,
      dynamoDBClient: Option[AmazonDynamoDB] = None,
      cloudWatchClient: Option[AmazonCloudWatch] = None,
      metricsFactory: Option[IMetricsFactory] = None,
      shardPrioritization: Option[ShardPrioritization] = None,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds
  )(implicit ec: ExecutionContext): Source[Record, NotUsed] =
    withoutCheckpoint(
      kinesisClientLibConfiguration,
      recordProcessorF,
      executionContextExecutorService,
      kinesisClient,
      dynamoDBClient,
      cloudWatchClient,
      metricsFactory,
      shardPrioritization,
      checkWorkerPeriodicity
    ).via(KCLFlow.ofCheckpoint())

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      recordProcessorF: RecordProcessorF = RecordProcessor,
      executionContextExecutorService: Option[ExecutionContextExecutorService] = None,
      kinesisClient: Option[AmazonKinesis] = None,
      dynamoDBClient: Option[AmazonDynamoDB] = None,
      cloudWatchClient: Option[AmazonCloudWatch] = None,
      metricsFactory: Option[IMetricsFactory] = None,
      shardPrioritization: Option[ShardPrioritization] = None,
      checkWorkerPeriodicity: FiniteDuration = 1 seconds
  )(implicit ec: ExecutionContext): Source[CommittableRecord, NotUsed] =
    Source.fromGraph(
      new KCLSourceStage(
        kinesisClientLibConfiguration,
        recordProcessorF,
        executionContextExecutorService,
        kinesisClient,
        dynamoDBClient,
        cloudWatchClient,
        metricsFactory,
        shardPrioritization,
        checkWorkerPeriodicity
      )
    )

}
