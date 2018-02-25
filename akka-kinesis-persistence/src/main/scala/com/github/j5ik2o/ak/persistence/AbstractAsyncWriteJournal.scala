package com.github.j5ik2o.ak.persistence

import akka.actor.ActorLogging
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.github.j5ik2o.ak.persistence.lifecycle.JournalRowRepository
import com.github.j5ik2o.ak.persistence.model.JournalRow
import com.github.j5ik2o.ak.persistence.serialization.FlowPersistentReprSerializer

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

abstract class AbstractAsyncWriteJournal extends AsyncWriteJournal with ActorLogging {

  type AkkaPersistenceId  = String
  type AkkaSequenceNumber = Long

  protected val serialization: Serialization = SerializationExtension(context.system)

  protected val serializer: FlowPersistentReprSerializer[JournalRow]

  protected val repository: JournalRowRepository

  import context.dispatcher

  protected def serializeMessages(messages: immutable.Seq[AtomicWrite]): (Seq[JournalRow], () => Seq[Try[Unit]]) = {
    val serializedTries = serializer.serialize(messages)
    val rowsToWrite = for {
      serializeTry <- serializedTries
      row          <- serializeTry.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete =
      if (serializedTries.forall(_.isSuccess)) Nil else serializedTries.map(_.map(_ => ()))
    (rowsToWrite, resultWhenWriteComplete _)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($messages)")
    val (rowsToWrite, resultWhenWriteComplete) = serializeMessages(messages)
    repository.storeMulti(rowsToWrite).map(_ => resultWhenWriteComplete().toVector)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug(s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr)")
    repository.deleteByPersistenceIdWithToSeqNr(persistenceId, toSequenceNr)
  }

  protected def callbackPersistentReprHandler(
      journalRows: Seq[JournalRow]
  )(recoveryCallback: PersistentRepr => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val mat = ActorMaterializer()
    Source(journalRows.toVector)
      .via(serializer.deserializeFlowWithoutTags)
      .fold(Try(Seq.empty[PersistentRepr])) {
        case (persistentReprTries, persistentReprTry) =>
          for {
            persistentReprs <- persistentReprTries
            persistentRepr  <- persistentReprTry
          } yield persistentReprs :+ persistentRepr
      }
      .runForeach { persistentReprTries =>
        persistentReprTries.foreach { persistentReprs =>
          persistentReprs.toList.sortBy(_.sequenceNr).foreach(recoveryCallback)
        }
      }
      .map(_ => ())
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug(s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max)")
    repository.resolveByPersistenceIdWithFromSeqNrWithMax(persistenceId, fromSequenceNr, toSequenceNr, max).flatMap {
      journalRows =>
        callbackPersistentReprHandler(journalRows)(recoveryCallback)
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    repository.resolveByLastSeqNr(persistenceId, fromSequenceNr).map(_.getOrElse(0L))
  }

}
