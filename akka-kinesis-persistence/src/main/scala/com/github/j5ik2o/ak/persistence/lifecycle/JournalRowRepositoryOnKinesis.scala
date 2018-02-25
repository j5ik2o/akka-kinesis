package com.github.j5ik2o.ak.persistence.lifecycle
import java.nio.ByteBuffer

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
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object JournalRowRepositoryOnKinesis {

  type KinesisShardId        = String
  type KinesisSequenceNumber = String

  case class KinesisShardIdWithSeqNr(kinesisShardId: KinesisShardId, kinesisSeqNr: KinesisSequenceNumber)
  case class KinesisShardIdWithSeqNrRange(kinesisShardId: KinesisShardId,
                                          fromKinesisSeqNr: KinesisSequenceNumber,
                                          toKinesisSeqNr: KinesisSequenceNumber)
}

class JournalRowRepositoryOnKinesis(streamName: String, regions: Regions, numOfShards: Int)(
    implicit system: ActorSystem
) extends JournalRowRepository {

  import JournalRowRepositoryOnKinesis._
  import io.circe.generic.auto._
  import io.circe.parser._
  import io.circe.syntax._

  private val logger = LoggerFactory.getLogger(getClass)

  implicit val mat = ActorMaterializer()

  private val kinesisShardIdWithSeqNrs
    : mutable.Map[AkkaPersistenceId, Map[AkkaSequenceNumber, KinesisShardIdWithSeqNr]] =
    mutable.Map.empty

  private val awsKinesisClient = new AwsKinesisClient(AwsClientConfig(regions))

  private val lastSeqNrs = mutable.Map.empty[AkkaPersistenceId, AkkaSequenceNumber]
  private var deletions  = Map.empty[AkkaPersistenceId, AkkaSequenceNumber]

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

  private def putLastSeqNr(journalRow: JournalRow): lastSeqNrs.type = {
    lastSeqNrs += (journalRow.id.persistenceId -> journalRow.id.sequenceNr)
  }

  private def putKinesisShardIdWithSeqNr(journalRow: JournalRow, userRecordResult: UserRecordResult) = {
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

  private def toUserRecord(journalRow: JournalRow): UserRecord = {
    toUserRecord(Seq(journalRow))
  }

  private def toUserRecord(journalRows: Seq[JournalRow]): UserRecord = {
    val messageAsJsonString = journalRows.asJson.noSpaces
    new UserRecord(streamName,
                   journalRows.head.id.persistenceId,
                   ByteBuffer.wrap(messageAsJsonString.getBytes("UTF-8")))
  }

  private def toJournalRows(record: Record): Try[Seq[JournalRow]] = {
    parse(new String(record.getData.array(), "UTF-8")) match {
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

  private def toJournalRows(records: Seq[Record]): Try[Seq[JournalRow]] = {
    records.foldLeft(Try(Seq.empty[JournalRow])) {
      case (resultTries, element) =>
        for {
          results <- resultTries
          result  <- toJournalRows(element)
        } yield results ++ result
    }
  }

  override def store(journalRow: JournalRow)(implicit ec: ExecutionContext): Future[Unit] = {
    Source
      .single(journalRow)
      .map { journalRow =>
        (toUserRecord(journalRow), Seq(journalRow))
      }
      .via(kplWithJournalRowsFlow)
      .mapAsync(1) {
        case (userRecordResult, Seq(_journalRow)) =>
          if (userRecordResult.isSuccessful) {
            putLastSeqNr(_journalRow)
            putKinesisShardIdWithSeqNr(_journalRow, userRecordResult)
            Future.successful(())
          } else {
            val detailMessage: String = toDetailMessage(userRecordResult)
            Future.failed(new Exception(s"occurred errors: $detailMessage"))
          }
      }
      .runWith(Sink.head)
  }

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
            _journalRows.foreach(putLastSeqNr)
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
    Future.successful { lastSeqNrs.get(persistenceId).filter(_ > fromSequenceNr) }

  override def deleteByPersistenceIdWithToSeqNr(persistenceId: AkkaPersistenceId, toSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Unit] = Future.successful { deletions = deletions + (persistenceId -> toSequenceNr) }

  override def resolveByPersistenceIdWithFromSeqNrWithMax(
      persistenceId: AkkaPersistenceId,
      fromSequenceNr: AkkaSequenceNumber,
      toSequenceNr: AkkaSequenceNumber,
      max: AkkaSequenceNumber
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
      val r = _readSizeOpt
        .map { _readSize =>
          val request = new GetRecordsRequest().withShardIterator(shardIterator).withLimit(_readSize)
          logger.debug(s"request = $request")
          awsKinesisClient
            .getRecordsAsync(request)
            .flatMap { getRecordResponse =>
              logger.debug(s"getRecordResponse = $getRecordResponse")
              Option(getRecordResponse.getRecords) match {
                case None =>
                  logger.debug(s"None case")
                  acc
                case Some(records) =>
                  logger.debug(s"Some nonEmpty case: $records")
                  val journalRowsTry: Try[Vector[JournalRow]] = toJournalRows(records.asScala).map { journalRows =>
                    logger.debug(s"journals = $journalRows")
                    journalRows.filter { v =>
                      val toSeqNrDeletedOpt = deletions.get(v.id.persistenceId)
                      fromSequenceNr <= v.id.sequenceNr && v.id.sequenceNr <= toSequenceNr && toSeqNrDeletedOpt.fold(
                        true
                      )(_ < v.id.sequenceNr)
                    }.toVector
                  }
                  journalRowsTry match {
                    case Success(journalRows) =>
                      logger.debug(s"current journalRows = $journalRows")
                      if (journalRows.exists(_.id.sequenceNr == toSequenceNr)) {
                        logger.debug("case-1")
                        acc.map(_ ++ journalRows.splitAt(_readSize)._1)
                      } else if (Option(getRecordResponse.getNextShardIterator).nonEmpty && journalRows.nonEmpty) {
                        logger.debug("case-2")
                        Thread.sleep(5 * 1000)
                        go(getRecordResponse.getNextShardIterator, readSize - journalRows.size, acc)
                      } else {
                        logger.debug("case-3")
                        acc.map(_ ++ journalRows)
                      }
                    case Failure(ex) =>
                      Future.failed(ex)
                  }
              }
            }
        }
        .getOrElse(acc)
      r
    }

    getShardIteratorFuture
      .flatMap { shardIdIteratorResponse =>
        go(shardIdIteratorResponse.getShardIterator, max, Future.successful(Seq.empty))
      }
      .map(_.toVector)
  }
}
