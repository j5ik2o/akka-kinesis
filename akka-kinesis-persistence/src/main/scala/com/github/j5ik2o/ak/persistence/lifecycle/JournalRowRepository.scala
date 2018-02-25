package com.github.j5ik2o.ak.persistence.lifecycle

import com.github.j5ik2o.ak.persistence.model.JournalRow

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

trait JournalRowRepository {

  type AkkaPersistenceId  = String
  type AkkaSequenceNumber = Long

  def store(journalRow: JournalRow)(implicit ec: ExecutionContext): Future[Unit]

  def storeMulti(journalRows: Seq[JournalRow])(implicit ec: ExecutionContext): Future[Unit]

  def resolveByLastSeqNr(persistenceId: AkkaPersistenceId, fromSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Option[AkkaSequenceNumber]]

  def deleteByPersistenceIdWithToSeqNr(persistenceId: AkkaPersistenceId, toSequenceNr: AkkaSequenceNumber)(
      implicit ec: ExecutionContext
  ): Future[Unit]

  def resolveByPersistenceIdWithFromSeqNrWithMax(
      persistenceId: AkkaPersistenceId,
      fromSequenceNr: AkkaSequenceNumber,
      toSequenceNr: AkkaSequenceNumber,
      max: Long
  )(implicit ec: ExecutionContext): Future[immutable.Seq[JournalRow]]

}

//trait JournalRowWriteQueueSupport {
//  this: JournalRowRepository =>
//
//  val bufferSize: Int
//  val batchSize: Int
//  val parallelism: Int
//
//  protected val writeQueue: SourceQueueWithComplete[(Promise[Unit], Seq[JournalRow])] = Source
//    .queue[(Promise[Unit], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
//    .batchWeighted[(Seq[Promise[Unit]], Seq[JournalRow])](batchSize, _._2.size, tup => Vector(tup._1) -> tup._2) {
//      case ((promises, rows), (newPromise, newRows)) => (promises :+ newPromise) -> (rows ++ newRows)
//    }
//    .mapAsync(parallelism) {
//      case (promises, rows) =>
//        storeMulti(rows)
//          .map(unit => promises.foreach(_.success(unit)))
//          .recover { case t => promises.foreach(_.failure(t)) }
//    }
//    .toMat(Sink.ignore)(Keep.left)
//    .run()
//
//  def storeMultiAsync(journalRows: Seq[JournalRow]): Future[Unit] = {
//    queueWriteJournalRows(journalRows)
//  }
//
//  private def queueWriteJournalRows(xs: Seq[JournalRow]): Future[Unit] = {
//    val promise = Promise[Unit]()
//    writeQueue.offer(promise -> xs).flatMap {
//      case QueueOfferResult.Enqueued =>
//        promise.future
//      case QueueOfferResult.Failure(t) =>
//        Future.failed(new Exception("Failed to write journal row batch", t))
//      case QueueOfferResult.Dropped =>
//        Future.failed(
//          new Exception(
//            s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
//          )
//        )
//      case QueueOfferResult.QueueClosed =>
//        Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
//    }
//  }
//
//}
