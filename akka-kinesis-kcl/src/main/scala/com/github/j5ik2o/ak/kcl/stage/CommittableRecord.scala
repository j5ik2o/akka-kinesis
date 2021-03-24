package com.github.j5ik2o.ak.kcl.stage

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.RecordProcessor

import scala.concurrent.{ ExecutionContext, Future }

final class CommittableRecord(
    val shardId: String,
    val recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
    val millisBehindLatest: Long,
    val record: Record,
    recordProcessor: RecordProcessor,
    checkPointer: IRecordProcessorCheckpointer
) {

  val sequenceNumber: String = record.getSequenceNumber

  def recordProcessorShutdownReason(): Option[ShutdownReason] =
    recordProcessor.maybeShutdownReason

  def canBeCheckpointed(): Boolean =
    recordProcessorShutdownReason().isEmpty

  def checkpoint()(implicit executor: ExecutionContext): Future[Unit] =
    Future(checkPointer.checkpoint(record))

}

object CommittableRecord {

  implicit val orderBySequenceNumber: Ordering[CommittableRecord] = Ordering.by(_.sequenceNumber)

}
