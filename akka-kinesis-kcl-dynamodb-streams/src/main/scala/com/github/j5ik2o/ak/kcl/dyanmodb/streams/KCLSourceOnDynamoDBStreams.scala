package com.github.j5ik2o.ak.kcl.dyanmodb.streams

import akka.stream.scaladsl.Source
import akka.stream.stage.AsyncCallback
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClient }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClient, AmazonDynamoDBStreamsClient }
import com.amazonaws.services.dynamodbv2.streamsadapter.{ AmazonDynamoDBStreamsAdapterClient, StreamsWorkerFactory }
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, Worker }
import com.amazonaws.services.kinesis.clientlibrary.types.{ InitializationInput, ShutdownInput }
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.dsl.KCLSource
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.RecordSet
import com.github.j5ik2o.ak.kcl.stage.{ CommittableRecord, KCLSourceStage }

import java.util.concurrent.ExecutorService
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

object KCLSourceOnDynamoDBStreams {

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      amazonDynamoDBStreamsAdapterClient: AmazonDynamoDBStreamsAdapterClient,
      amazonDynamoDBClient: AmazonDynamoDBClient,
      amazonCloudWatchClientOpt: Option[AmazonCloudWatchClient],
      metricsFactoryOpt: Option[IMetricsFactory],
      executorService: ExecutorService,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] =
    KCLSource
      .ofCustomWorker(
        checkWorkerPeriodicity, {
          (
              onInitializeCallback: AsyncCallback[InitializationInput],
              onRecordCallback: AsyncCallback[RecordSet],
              onShutdownCallback: AsyncCallback[ShutdownInput]
          ) =>
            (amazonCloudWatchClientOpt, metricsFactoryOpt) match {
              case (Some(amazonCloudWatchClient), None) =>
                StreamsWorkerFactory.createDynamoDbStreamsWorker(
                  recordProcessorFactoryOpt.getOrElse {
                    KCLSourceStage.newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
                  },
                  kinesisClientLibConfiguration,
                  amazonDynamoDBStreamsAdapterClient,
                  amazonDynamoDBClient,
                  amazonCloudWatchClient,
                  executorService
                )
              case (None, Some(metricsFactory)) =>
                StreamsWorkerFactory.createDynamoDbStreamsWorker(
                  recordProcessorFactoryOpt.getOrElse {
                    KCLSourceStage.newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
                  },
                  kinesisClientLibConfiguration,
                  amazonDynamoDBStreamsAdapterClient,
                  amazonDynamoDBClient,
                  metricsFactory,
                  executorService
                )
              case _ =>
                throw new IllegalArgumentException("Please select either amazonCloudWatchClient or metricsFactory.")
            }
        }
      )

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      amazonDynamoDBStreamsAdapterClient: AmazonDynamoDBStreamsAdapterClient,
      amazonDynamoDB: AmazonDynamoDB,
      amazonCloudWatchClientOpt: Option[AmazonCloudWatch],
      metricsFactoryOpt: Option[IMetricsFactory],
      execService: ExecutorService,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] = {
    KCLSource.ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity, {
        (
            onInitializeCallback: AsyncCallback[InitializationInput],
            onRecordCallback: AsyncCallback[RecordSet],
            onShutdownCallback: AsyncCallback[ShutdownInput]
        ) =>
          (amazonCloudWatchClientOpt, metricsFactoryOpt) match {
            case (Some(amazonCloudWatchClient), None) =>
              StreamsWorkerFactory.createDynamoDbStreamsWorker(
                recordProcessorFactoryOpt.getOrElse {
                  KCLSourceStage.newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
                },
                kinesisClientLibConfiguration,
                amazonDynamoDBStreamsAdapterClient,
                amazonDynamoDB,
                amazonCloudWatchClient,
                execService
              )
            case (None, Some(metricsFactory)) =>
              StreamsWorkerFactory.createDynamoDbStreamsWorker(
                recordProcessorFactoryOpt.getOrElse {
                  KCLSourceStage.newRecordProcessorFactory(onInitializeCallback, onRecordCallback, onShutdownCallback)
                },
                kinesisClientLibConfiguration,
                amazonDynamoDBStreamsAdapterClient,
                amazonDynamoDB,
                metricsFactory,
                execService
              )
            case _ =>
              throw new IllegalArgumentException("Please select either amazonCloudWatchClient or metricsFactory.")
          }
      }
    )
  }
}
