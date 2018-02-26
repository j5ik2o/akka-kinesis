package com.github.j5ik2o.ak.persistence.lifecycle

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, GraphDSL, Sink, Source, Unzip, Zip }
import akka.stream.{ ActorMaterializer, FlowShape }
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.{ GetRecordsRequest, GetShardIteratorRequest, Record, ShardIteratorType }
import com.amazonaws.services.kinesis.producer.{
  KinesisProducer,
  KinesisProducerConfiguration,
  UserRecord,
  UserRecordResult
}
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import com.github.j5ik2o.ak.persistence.model.JournalRow
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{ immutable, mutable }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object JournalRowRepositoryOnKinesis {

  def lruCache[A, B](maxEntries: Int): mutable.Map[A, B] =
    new java.util.LinkedHashMap[A, B]() {
      override def removeEldestEntry(eldest: java.util.Map.Entry[A, B]) = size > maxEntries
    }.asScala

  final val CACHE_SIZE = 1000

  type KinesisShardId        = String
  type KinesisSequenceNumber = String

  case class KinesisShardIdWithSeqNr(kinesisShardId: KinesisShardId,
                                     kinesisSeqNr: KinesisSequenceNumber,
                                     deleted: Boolean = false)

  case class KinesisShardIdWithSeqNrRange(kinesisShardId: KinesisShardId,
                                          fromKinesisSeqNr: KinesisSequenceNumber,
                                          toKinesisSeqNr: KinesisSequenceNumber)

}

class JournalRowRepositoryOnKinesis(streamName: String,
                                    regions: Regions,
                                    numOfShards: Int,
                                    nextShardIteratorInterval: FiniteDuration = 3 seconds)(
    implicit system: ActorSystem
) extends JournalRowRepository {

  import JournalRowRepositoryOnKinesis._
  import io.circe.generic.auto._
  import io.circe.parser._
  import io.circe.syntax._

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val mat = ActorMaterializer()

  private val kinesisShardIdWithSeqNrs
    : mutable.Map[AkkaPersistenceId, Map[AkkaSequenceNumber, KinesisShardIdWithSeqNr]] = lruCache(CACHE_SIZE)

  private val awsKinesisClient = new AwsKinesisClient(AwsClientConfig(regions))

  private val deletions: mutable.Map[AkkaPersistenceId, AkkaSequenceNumber] = lruCache(CACHE_SIZE)

  private val kinesisProducerConfiguration: KinesisProducerConfiguration = new KinesisProducerConfiguration()
    .setRegion(regions.getName)
    .setCredentialsRefreshDelay(100)

  private val kplFlowSettings: KPLFlowSettings = KPLFlowSettings.byNumberOfShards(numOfShards)

  private def kplFlow(implicit ec: ExecutionContext): Flow[UserRecord, UserRecordResult, Future[KinesisProducer]] =
    KPLFlow(streamName, kinesisProducerConfiguration, kplFlowSettings)

  private def kplWithJournalRowsFlow(implicit ec: ExecutionContext) =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val unzip = b.add(Unzip[UserRecord, Seq[JournalRow]])
      val zip   = b.add(Zip[UserRecordResult, Seq[JournalRow]])
      unzip.out0 ~> kplFlow ~> zip.in0
      unzip.out1 ~> zip.in1
      FlowShape(unzip.in, zip.out)
    })

  private def putKinesisShardIdWithSeqNr(
      journalRow: JournalRow,
      userRecordResult: UserRecordResult
  ): Option[Map[AkkaSequenceNumber, KinesisShardIdWithSeqNr]] = {
    val values = kinesisShardIdWithSeqNrs.getOrElseUpdate(journalRow.id.persistenceId, Map.empty)
    kinesisShardIdWithSeqNrs.put(
      journalRow.id.persistenceId,
      values + (journalRow.id.sequenceNr -> KinesisShardIdWithSeqNr(userRecordResult.getShardId,
                                                                    userRecordResult.getSequenceNumber))
    )
  }

  private def getSeqNrRange(persistenceId: AkkaPersistenceId,
                            fromSequenceNr: AkkaSequenceNumber,
                            toSequenceNr: AkkaSequenceNumber): KinesisShardIdWithSeqNrRange = {
    val values                                 = kinesisShardIdWithSeqNrs(persistenceId)
    val metadatas                              = values.filter { case (key, _) => fromSequenceNr <= key && key <= toSequenceNr }.values
    val kinesisShardId                         = metadatas.head.kinesisShardId
    val kinesisMinSeqNr: KinesisSequenceNumber = metadatas.minBy(_.kinesisSeqNr).kinesisSeqNr
    val kinesisMaxSeqNr                        = metadatas.maxBy(_.kinesisSeqNr).kinesisSeqNr
    KinesisShardIdWithSeqNrRange(kinesisShardId, kinesisMinSeqNr, kinesisMaxSeqNr)
  }

  private def toUserRecord(journalRows: Seq[JournalRow]): UserRecord = {
    def encodeJournalRows(journalRows: Seq[JournalRow]) =
      ByteBuffer.wrap(journalRows.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    new UserRecord(streamName, journalRows.head.id.persistenceId, encodeJournalRows(journalRows))
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

  private def toJournalRowsTry(records: Seq[Record]): Try[Seq[JournalRow]] = {
    records.foldLeft(Try(Seq.empty[JournalRow])) {
      case (resultTries, element) =>
        for {
          results <- resultTries
          result  <- toJournalRowsTry(element)
        } yield results ++ result
    }
  }

  private def resolveFilteredJournalRows(journalRows: Seq[JournalRow],
                                         fromSequenceNr: AkkaSequenceNumber,
                                         toSequenceNr: AkkaSequenceNumber): Future[Seq[JournalRow]] = {
    Future.successful {
      journalRows.filter { journalRow =>
        val toSeqNrDeletedOpt = deletions.get(journalRow.id.persistenceId)
        fromSequenceNr <= journalRow.id.sequenceNr && journalRow.id.sequenceNr <= toSequenceNr && toSeqNrDeletedOpt
          .fold(
            true
          )(_ < journalRow.id.sequenceNr)
      }
    }
  }

  override def store(journalRow: JournalRow)(implicit ec: ExecutionContext): Future[Unit] =
    storeMulti(Seq(journalRow))

  override def storeMulti(journalRows: Seq[JournalRow])(implicit ec: ExecutionContext): Future[Unit] = {
    Source
      .single(journalRows)
      .map { journalRows =>
        (toUserRecord(journalRows), journalRows)
      }
      .via(kplWithJournalRowsFlow)
      .mapAsync(1) {
        case (userRecordResult, _journalRows) =>
          if (userRecordResult.isSuccessful) {
            _journalRows.foreach(j => putKinesisShardIdWithSeqNr(j, userRecordResult))
            Future.successful(())
          } else {
            val detailMessage: String = toDetailMessage(userRecordResult)
            Future.failed(new Exception(s"occurred errors: $detailMessage"))
          }
      }
      .runWith(Sink.head)
  }

  private def toDetailMessage(userRecordResult: UserRecordResult) = {
    val detailMessage = userRecordResult.getAttempts.asScala.zipWithIndex
      .map {
        case (attempt, idx) =>
          s"$idx) errorCode = ${attempt.getErrorCode}, errorMessage = ${attempt.getErrorMessage}}"
      }
      .mkString(", ")
    detailMessage
  }

  override def resolveByLastSeqNr(persistenceId: AkkaPersistenceId, fromSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Option[AkkaSequenceNumber]] =
    Future.successful {
      kinesisShardIdWithSeqNrs.get(persistenceId).flatMap { values =>
        values.filter(_._1 > fromSequenceNr).toList match {
          case Nil =>
            None
          case seq =>
            Some(seq.maxBy(_._1)._1)
        }
      }
    }

  override def deleteByPersistenceIdWithToSeqNr(persistenceId: AkkaPersistenceId, toSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Unit] = Future.successful {

    deletions.put(persistenceId, toSequenceNr)
  }

  override def resolveByPersistenceIdWithFromSeqNrWithMax(
      persistenceId: AkkaPersistenceId,
      fromSequenceNr: AkkaSequenceNumber,
      toSequenceNr: AkkaSequenceNumber,
      max: Long
  )(implicit ec: ExecutionContext): Future[immutable.Seq[JournalRow]] = {
    logger.debug(s"resolveByPersistenceIdWithFromSeqNrWithMax($persistenceId, $fromSequenceNr, $toSequenceNr)")
    val KinesisShardIdWithSeqNrRange(kinesisShardId, kinesisMinSeqNr, kinesisMaxSeqNr) =
      getSeqNrRange(persistenceId, fromSequenceNr, toSequenceNr)
    logger.debug(s"KinesisShardIdWithSeqNrRange($kinesisShardId, $kinesisMinSeqNr, $kinesisMaxSeqNr)")
    val getShardIteratorFuture = awsKinesisClient.getShardIteratorAsync(
      new GetShardIteratorRequest()
        .withStreamName(streamName)
        .withShardId(kinesisShardId)
        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
        .withStartingSequenceNumber(kinesisMinSeqNr)
    )

    def go(shardIterator: String, readSize: Long, acc: Future[Seq[JournalRow]]): Future[Seq[JournalRow]] = {
      logger.debug(s"go(shardIterator = $shardIterator, readSize = $readSize")
      val _readSizeOpt = readSize match {
        case r if r > 1000 => Some(1000)
        case 0             => None
        case r             => Some(r.toInt)
      }
      logger.debug(s"_readSizeOpt = ${_readSizeOpt}")
      _readSizeOpt
        .map { _readSize =>
          val request = new GetRecordsRequest().withShardIterator(shardIterator).withLimit(_readSize)
          logger.debug(s"request = $request")
          for {
            recordResponse <- awsKinesisClient.getRecordsAsync(request)
            journalRows <- if (Option(recordResponse.getRecords).isEmpty)
              Future.successful(Seq.empty)
            else
              for {
                journalRows         <- Future.fromTry(toJournalRowsTry(recordResponse.getRecords.asScala))
                filteredJournalRows <- resolveFilteredJournalRows(journalRows, fromSequenceNr, toSequenceNr)
              } yield filteredJournalRows
            result <- if (journalRows.exists(_.id.sequenceNr == toSequenceNr))
              acc.map(_ ++ journalRows.splitAt(_readSize)._1)
            else if (Option(recordResponse.getNextShardIterator).nonEmpty && journalRows.nonEmpty) {
              Thread.sleep(nextShardIteratorInterval.toMillis)
              go(recordResponse.getNextShardIterator, readSize - journalRows.size, acc)
            } else
              acc.map(_ ++ journalRows)
          } yield result
        }
        .getOrElse(acc)
    }

    getShardIteratorFuture
      .flatMap { shardIdIteratorResponse =>
        go(shardIdIteratorResponse.getShardIterator, max, Future.successful(Seq.empty))
      }
      .map(_.toVector)
  }
}
