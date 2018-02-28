package com.github.j5ik2o.ak.persistence

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.{ Cancellable, ExtendedActorSystem }
import akka.persistence.journal.EventAdapters
import akka.persistence.query.scaladsl._
import akka.persistence.query.{ EventEnvelope, Offset, Sequence }
import akka.persistence.{ Persistence, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Sink, Source }
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, Worker }
import com.amazonaws.services.kinesis.clientlibrary.types.{ InitializationInput, ShutdownInput }
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model._
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.github.j5ik2o.ak.kcl.dsl.KCLSource
import com.github.j5ik2o.ak.persistence.model.JournalRow
import com.github.j5ik2o.ak.persistence.serialization.{ ByteArrayJournalSerializer, FlowPersistentReprSerializer }
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

class KinesisScalaReadJournal(config: Config)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery {
  import io.circe.generic.auto._
  import io.circe.parser._

  implicit val ec: ExecutionContext = system.dispatcher

  implicit val mat = ActorMaterializer()

  private val streamName: String              = config.getString("stream-name")
  private val regions: Regions                = Regions.fromName(config.getString("region"))
  private val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").toMillis millis
  private val maxBufferSize: Int              = config.getInt("max-buffer-size")

  private val serialization: Serialization = SerializationExtension(system)

  private val serializer: FlowPersistentReprSerializer[JournalRow] = new ByteArrayJournalSerializer(
    serialization
  )

  private val writePluginId                = config.getString("write-plugin")
  private val eventAdapters: EventAdapters = Persistence(system).adaptersFor(writePluginId)

  private def adaptEvents(repr: PersistentRepr): Seq[PersistentRepr] = {
    val adapter = eventAdapters.get(repr.payload.getClass)
    adapter.fromJournal(repr.payload, repr.manifest).events.map(repr.withPayload)
  }

  private def toJournalRowsTry(record: Record): Try[Seq[JournalRow]] = {
    parse(new String(record.getData.array(), StandardCharsets.UTF_8)) match {
      case Right(parseResult) =>
        parseResult.as[Seq[JournalRow]] match {
          case Right(journalRows) =>
            Success(journalRows)
          case Left(error) =>
            Failure(new Exception(error.message))
        }
      case Left(error) =>
        Failure(new Exception(error.message))
    }
  }

  private val shardIteratorType = ShardIteratorType.TRIM_HORIZON
  private val awsKinesisClient  = new AwsKinesisClient(AwsClientConfig(regions))
  protected lazy val awsDynamoDBClient: AmazonDynamoDBAsync = AmazonDynamoDBAsyncClientBuilder
    .standard()
    .withRegion(regions)
    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
//    .withEndpointConfiguration(
//      new EndpointConfiguration(s"http://127.0.0.1:$dynamoDBPort", _region)
//    )
    .build()

  private val delaySource: Source[Int, Cancellable] =
    Source.tick(refreshInterval, 0.seconds, 0).take(1)

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
            .withShardIteratorType(shardIteratorType)
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

  override def persistenceIds(): Source[String, NotUsed] =
    Source
      .repeat(0)
      .flatMapConcat(_ => delaySource.flatMapConcat(_ => currentPersistenceIds()))
      .statefulMapConcat[String] { () =>
        var knownIds = Set.empty[String]
        def next(id: String): Iterable[String] = {
          val xs = Set(id).diff(knownIds)
          knownIds += id
          xs
        }
        (id) =>
          next(id)
      }

  private def currentJournalEventsByPersistenceId(persistenceId: String,
                                                  fromSequenceNr: Long,
                                                  toSequenceNr: Long): Source[PersistentRepr, NotUsed] =
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
            .withShardIteratorType(shardIteratorType)
      }
      .via(getShardIteratorFlow)
      .log("getShardIteratorAsync")
      .map { result =>
        new GetRecordsRequest().withShardIterator(result.getShardIterator)
      }
      .via(getRecordsFlow)
      .mapConcat { recordsResult =>
        recordsResult.getRecords.asScala.filter { v =>
          v.getPartitionKey == persistenceId
        }.toVector
      }
      .mapAsync(1) { record =>
        Future.fromTry(toJournalRowsTry(record))
      }
      .mapConcat { journalRows =>
        journalRows.filter { v =>
          fromSequenceNr <= v.id.sequenceNr && v.id.sequenceNr <= toSequenceNr
        }.toVector
      }
      .via(serializer.deserializeFlow)
      .mapAsync(1)(Future.fromTry)
      .map(_.persistentRepr)

  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    currentJournalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .mapConcat(adaptEvents)
      .map { persistentRepr =>
        EventEnvelope(
          Offset.sequence(persistentRepr.sequenceNr),
          persistentRepr.persistenceId,
          persistentRepr.sequenceNr,
          persistentRepr.payload
        )
      }
      .log("records")
  }

  override def eventsByPersistenceId(persistenceId: String,
                                     fromSequenceNr: Long,
                                     toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .unfoldAsync[Long, Seq[EventEnvelope]](Math.max(1, fromSequenceNr)) { (from: Long) =>
        def nextFromSeqNr(xs: Seq[EventEnvelope]): Long = {
          if (xs.isEmpty) from else xs.map(_.sequenceNr).max + 1
        }
        from match {
          case x if x > toSequenceNr => Future.successful(None)
          case _ =>
            delaySource
              .flatMapConcat { _ =>
                currentJournalEventsByPersistenceId(persistenceId, from, toSequenceNr)
                  .take(maxBufferSize)
              }
              .mapConcat(adaptEvents)
              .map(repr => EventEnvelope(Sequence(repr.sequenceNr), repr.persistenceId, repr.sequenceNr, repr.payload))
              .runWith(Sink.seq)
              .map { xs =>
                val newFromSeqNr = nextFromSeqNr(xs)
                Some((newFromSeqNr, xs))
              }
        }
      }
      .mapConcat(identity)
  }

  def eventsByKCL(
      config: KinesisClientLibConfiguration,
  ): Source[EventEnvelope, Future[Worker]] = {
    KCLSource(config,
              kinesisClient = Some(awsKinesisClient.underlying),
              dynamoDBClient = Some(awsDynamoDBClient),
              metricsFactory = Some(new NullMetricsFactory))
      .mapAsync(1) { record =>
        Future.fromTry(toJournalRowsTry(record))
      }
      .mapConcat(_.toVector)
      .via(serializer.deserializeFlow)
      .mapAsync(1)(Future.fromTry)
      .map(_.persistentRepr)
      .mapConcat(adaptEvents)
      .map { persistentRepr =>
        EventEnvelope(
          Offset.sequence(persistentRepr.sequenceNr),
          persistentRepr.persistenceId,
          persistentRepr.sequenceNr,
          persistentRepr.payload
        )
      }
  }

}
