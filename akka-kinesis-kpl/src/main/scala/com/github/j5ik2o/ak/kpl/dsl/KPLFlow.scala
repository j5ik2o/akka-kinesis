package com.github.j5ik2o.ak.kpl.dsl

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import com.amazonaws.services.kinesis.model.{ PutRecordsRequestEntry, PutRecordsResultEntry }
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord }
import com.github.j5ik2o.ak.kpl.stage.KPLFlowStage

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object KPLFlow {

  def apply(streamName: String, kinesisProducerConfiguration: KinesisProducerConfiguration, settings: KPLFlowSettings)(
      implicit ec: ExecutionContext
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .throttle(settings.maxRecordsPerSecond, 1 second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
      .throttle(settings.maxBytesPerSecond, 1 second, settings.maxBytesPerSecond, getByteSize, ThrottleMode.Shaping)
      .mapAsync(settings.parallelism) { v =>
        Future {
          new UserRecord(streamName, v.getPartitionKey, v.getExplicitHashKey, v.getData)
        }
      }
      .groupBy(settings.parallelism, _.getPartitionKey.## % settings.parallelism)
      .via(
        new KPLFlowStage(
          kinesisProducerConfiguration,
          settings.maxRetries,
          settings.backoffStrategy,
          settings.retryInitialTimeout
        )
      )
      .log("kpl")
      .mapAsync(settings.parallelism) { v =>
        Future {
          new PutRecordsResultEntry()
            .withShardId(v.getShardId)
            .withSequenceNumber(v.getSequenceNumber)
        }
      }
      .mergeSubstreams

  private def getByteSize(record: PutRecordsRequestEntry): Int =
    record.getPartitionKey.length + record.getData.position
}
