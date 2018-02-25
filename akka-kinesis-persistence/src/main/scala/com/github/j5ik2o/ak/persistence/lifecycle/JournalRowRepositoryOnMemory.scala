package com.github.j5ik2o.ak.persistence.lifecycle
import com.github.j5ik2o.ak.persistence.model.JournalRow

import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }

class JournalRowRepositoryOnMemory extends JournalRowRepository {

  private val journals  = mutable.Map.empty[AkkaPersistenceId, mutable.Map[AkkaSequenceNumber, JournalRow]]
  private var deletions = Map.empty[AkkaPersistenceId, AkkaSequenceNumber]

  override def store(journalRow: JournalRow)(implicit ec: ExecutionContext): Future[Unit] = Future.successful {
    val body = journals.getOrElseUpdate(journalRow.id.persistenceId, mutable.Map.empty)
    body.put(journalRow.id.sequenceNr, journalRow)
    journals.put(journalRow.id.persistenceId, body)
  }

  override def storeMulti(journalRows: Seq[JournalRow])(implicit ec: ExecutionContext): Future[Unit] = {
    val futures = journalRows.map { journalRow =>
      store(journalRow)
    }
    Future.sequence(futures).map(_ => ())
  }

  override def deleteByPersistenceIdWithToSeqNr(persistenceId: AkkaPersistenceId, toSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Unit] =
    Future.successful {
      deletions = deletions + (persistenceId -> toSequenceNr)
    }

  override def resolveByPersistenceIdWithFromSeqNrWithMax(
      persistenceId: String,
      fromSequenceNr: AkkaSequenceNumber,
      toSequenceNr: AkkaSequenceNumber,
      max: Long
  )(implicit ec: ExecutionContext): Future[immutable.Seq[JournalRow]] =
    Future.successful {
      if (max == 0) immutable.Seq.empty
      else {
        val count = if (max >= Int.MaxValue) Int.MaxValue else max.toInt
        journals(persistenceId)
          .filter {
            case (seqNr, journalRow) =>
              val toSeqNrDeletedOpt = deletions.get(journalRow.id.persistenceId)
              (fromSequenceNr <= seqNr && seqNr <= toSequenceNr) && toSeqNrDeletedOpt
                .fold(true)(_ < journalRow.id.sequenceNr)
          }
          .values
          .toList
          .sortBy(_.id.sequenceNr)
          .take(count)
          .toVector
      }
    }

  override def resolveByLastSeqNr(persistenceId: AkkaPersistenceId, fromSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Option[AkkaSequenceNumber]] =
    Future.successful {
      journals.get(persistenceId).map { journalRows =>
        journalRows.toSeq.filter(_._1 > fromSequenceNr).maxBy(_._1)._1
      }
    }

}
