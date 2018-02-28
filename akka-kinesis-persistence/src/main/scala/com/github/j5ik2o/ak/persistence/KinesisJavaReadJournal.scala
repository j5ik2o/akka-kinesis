package com.github.j5ik2o.ak.persistence

import java.util.concurrent.Future

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, Worker }
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.aws.JavaFutureConverter._

class KinesisJavaReadJournal(underling: KinesisScalaReadJournal)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery {

  implicit val system = underling.system
  import system.dispatcher

  override def currentPersistenceIds(): Source[String, NotUsed] = underling.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    underling.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def persistenceIds(): Source[String, NotUsed] = underling.persistenceIds().asJava

  override def eventsByPersistenceId(persistenceId: String,
                                     fromSequenceNr: Long,
                                     toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    underling.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  def eventsByKCL(config: KinesisClientLibConfiguration): Source[EventEnvelope, Future[Worker]] =
    underling.eventsByKCL(config).asJava.mapMaterializedValue(_.toJava)

}
