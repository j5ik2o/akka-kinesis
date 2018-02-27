package com.github.j5ik2o.ak.persistence

import akka.actor.ExtendedActorSystem
import akka.persistence.query.javadsl.ReadJournal
import com.typesafe.config.Config

class KinesisJavaReadJournal(config: Config, system: ExtendedActorSystem) extends ReadJournal {}
