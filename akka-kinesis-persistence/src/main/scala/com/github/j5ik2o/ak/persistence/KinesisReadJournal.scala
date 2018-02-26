package com.github.j5ik2o.ak.persistence

import akka.NotUsed
import akka.persistence.query.scaladsl.{ CurrentPersistenceIdsQuery, PersistenceIdsQuery, ReadJournal }
import akka.stream.scaladsl.Source
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }

class KinesisReadJournal extends ReadJournal with CurrentPersistenceIdsQuery {
  val streamName: String       = ""
  private val awsKinesisClient = new AwsKinesisClient(AwsClientConfig())

  override def currentPersistenceIds(): Source[String, NotUsed] = { ??? }

//  implicit val mat = ActorMaterializer()
//
//  val iterator = new Iterator[String] {
//    override def hasNext: Boolean = true
//
//    override def next(): String = streamName
//  }

//  override def persistenceIds(): Source[String, NotUsed] = {
//    ???
//    Source
//      .fromIterator { () =>
//        iterator
//      }
//      .mapAsync(1)(v => awsKinesisClient.describeStreamAsync(v))
//      .mapConcat { describeStreamResult =>
//        describeStreamResult.getStreamDescription.getShards.asScala.map(_.getShardId).toVector
//      }
//      .mapAsync(1) { shardId =>
//        val request = new GetShardIteratorRequest()
//          .withStreamName(streamName)
//          .withShardId(shardId)
//          .withShardIteratorType(ShardIteratorType.LATEST)
//        awsKinesisClient.getShardIteratorAsync(request)
//      }
//      .mapAsync(1) { result =>
//        val request = new GetRecordsRequest().withShardIterator(result.getShardIterator)
//        awsKinesisClient.getRecordsAsync(request)
//      }
//      .mapConcat { result =>
//        result.getRecords.asScala.toVector.map(_.getPartitionKey)
//      }
  //}
}
