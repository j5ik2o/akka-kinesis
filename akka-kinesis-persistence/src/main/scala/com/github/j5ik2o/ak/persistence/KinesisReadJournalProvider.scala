package com.github.j5ik2o.ak.persistence

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.javadsl
import akka.persistence.query.scaladsl
import com.typesafe.config.Config

class KinesisReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override def scaladslReadJournal(): scaladsl.ReadJournal = new KinesisScalaReadJournal(config)(system)

  override def javadslReadJournal(): javadsl.ReadJournal = new KinesisJavaReadJournal(config, system)

}
