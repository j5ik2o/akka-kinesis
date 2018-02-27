package com.github.j5ik2o.ak.persistence

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentPersistenceIdsQuery,
  PersistenceIdsQuery,
  ReadJournal
}
import akka.stream.scaladsl.{ Flow, Source }
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model._
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

class KinesisReadJournal(streamName: String, regions: Regions)(implicit ec: ExecutionContext)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery {

  private val awsKinesisClient = new AwsKinesisClient(AwsClientConfig(regions))

  private val describeStreamFlow: Flow[DescribeStreamRequest, DescribeStreamResult, NotUsed] =
    Flow[DescribeStreamRequest].mapAsync(1)(awsKinesisClient.describeStreamAsync)

  private val getShardIteratorFlow: Flow[GetShardIteratorRequest, GetShardIteratorResult, NotUsed] =
    Flow[GetShardIteratorRequest].mapAsync(1)(awsKinesisClient.getShardIteratorAsync)

  private val getRecordsFlow: Flow[GetRecordsRequest, GetRecordsResult, NotUsed] =
    Flow[GetRecordsRequest].mapAsync(5) { request =>
      def go(getRecordsRequest: GetRecordsRequest): Future[GetRecordsResult] =
        awsKinesisClient.getRecordsAsync(getRecordsRequest).flatMap { result =>
          if (result.getRecords.isEmpty && result.getNextShardIterator != null) {
            println("next loop")
            Thread.sleep(3 * 1000)
            go(new GetRecordsRequest().withShardIterator(result.getNextShardIterator))
          } else {
            Future.successful(result)
          }
        }
      go(request)
    }

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    Source
      .single(new DescribeStreamRequest().withStreamName(streamName))
      .via(describeStreamFlow)
      .log("describeStreamAsync")
      .mapConcat { result =>
        result.getStreamDescription.getShards.asScala.map { shard =>
          (result.getStreamDescription.getStreamName, shard)
        }.toVector
      }
      .log("shard")
      .map {
        case (_streamName, shard) =>
          new GetShardIteratorRequest()
            .withStreamName(_streamName)
            .withShardId(shard.getShardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
      }
      .via(getShardIteratorFlow)
      .log("getShardIteratorAsync")
      .map { result =>
        new GetRecordsRequest().withShardIterator(result.getShardIterator)
      }
      .via(getRecordsFlow)
      .mapConcat { result =>
        result.getRecords.asScala.toVector.map(_.getPartitionKey)
      }
      .log("records")
  }

  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] = { ??? }
}
