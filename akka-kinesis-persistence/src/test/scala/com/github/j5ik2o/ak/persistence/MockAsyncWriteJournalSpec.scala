package com.github.j5ik2o.ak.persistence

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class MockAsyncWriteJournalSpec extends JournalSpec(ConfigFactory.load("mock-journal-spec.conf")) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = true

}
