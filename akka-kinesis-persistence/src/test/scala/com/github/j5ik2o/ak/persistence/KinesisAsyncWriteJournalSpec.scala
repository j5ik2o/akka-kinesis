package com.github.j5ik2o.ak.persistence

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.amazonaws.regions.Regions
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.typesafe.config.ConfigFactory

class KinesisAsyncWriteJournalSpec extends JournalSpec(ConfigFactory.load("journal-spec.conf")) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = true

  val region     = Regions.AP_NORTHEAST_1
  val client     = new AwsKinesisClient(AwsClientConfig(region))
  val streamName = sys.env("KPL_STREAM_NAME")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    client.createStream(streamName, 1)
    client.waitStreamToCreated(streamName)
  }

  override protected def afterAll(): Unit = {
    client.deleteStream(streamName)
    super.afterAll()
  }

}
