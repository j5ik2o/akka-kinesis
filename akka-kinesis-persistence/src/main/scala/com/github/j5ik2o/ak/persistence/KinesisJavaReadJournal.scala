package com.github.j5ik2o.ak.persistence

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source

class KinesisJavaReadJournal(underling: KinesisScalaReadJournal)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery {

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

}
