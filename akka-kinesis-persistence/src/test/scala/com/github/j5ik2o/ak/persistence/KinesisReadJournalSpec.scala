package com.github.j5ik2o.ak.persistence

import akka.actor.ActorSystem
import akka.persistence.PluginSpec
import akka.persistence.journal.{ JournalSpec, WriteJournalUtils }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ ImplicitSender, TestProbe }
import com.amazonaws.regions.Regions
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, Matchers }

class KinesisReadJournalSpec
    extends PluginSpec(ConfigFactory.load("journal-spec.conf"))
    with ImplicitSender
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {
  val streamName = sys.env("KPL_STREAM_NAME")
  val region     = Regions.AP_NORTHEAST_1
  val client     = new AwsKinesisClient(AwsClientConfig(region))

  implicit val mat                      = ActorMaterializer()
  implicit lazy val system: ActorSystem = ActorSystem("KinesisReadJournalSpec", config.withFallback(JournalSpec.config))

  import system.dispatcher

  var writeJournalUtils: WriteJournalUtils = _
  var readJournal: KinesisReadJournal      = _
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    client.createStream(streamName, 1)
    client.waitStreamToCreated(streamName)
    writeJournalUtils = new WriteJournalUtils(extension)
    readJournal = new KinesisReadJournal(streamName, region)
  }

  override protected def afterAll(): Unit = {
    client.deleteStream(streamName)
    shutdown(system)
  }

  "KinesisReadJournal" must {
    "currentPersistenceIds" in {
      val probe = TestProbe()
      writeJournalUtils.writeMessages(0, 10, "a-1", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe)
      val persistenceIds = readJournal.currentPersistenceIds().runWith(Sink.seq).futureValue
      persistenceIds shouldBe Seq("a-1")
    }
  }
}
