package com.github.j5ik2o.ak.persistence

import akka.actor.ActorSystem
import akka.persistence.PluginSpec
import akka.persistence.journal.{ JournalSpec, WriteJournalUtils }
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ ImplicitSender, TestProbe }
import com.amazonaws.regions.Regions
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }

class KinesisScalaReadJournalSpec
    extends PluginSpec(ConfigFactory.load("journal-spec.conf"))
    with ImplicitSender
    with Matchers
    with ScalaFutures {
  val streamName = sys.env("KPL_STREAM_NAME")
  val region     = Regions.AP_NORTHEAST_1
  val client     = new AwsKinesisClient(AwsClientConfig(region))

  implicit val mat                      = ActorMaterializer()
  implicit lazy val system: ActorSystem = ActorSystem("KinesisReadJournalSpec", config.withFallback(JournalSpec.config))

  var writeJournalUtils: WriteJournalUtils = _
  var readJournal: KinesisScalaReadJournal = _

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    client.createStream(streamName, 1)
    client.waitStreamToCreated(streamName)
  }

  protected override def afterAll(): Unit = {
    client.deleteStream(streamName)
    shutdown(system)
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    writeJournalUtils = new WriteJournalUtils(extension)
    readJournal = PersistenceQuery(system).readJournalFor[KinesisScalaReadJournal]("kinesis-read-journal")
  }

  override protected def afterEach(): Unit = {}

  "KinesisReadJournal" must {
    "currentPersistenceIds" in {
      val probe = TestProbe()
      writeJournalUtils.writeMessages(0, 9, "a-1", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
      val persistenceIds = readJournal.currentPersistenceIds().runWith(Sink.seq).futureValue
      persistenceIds shouldBe Seq("a-1")
    }
    "currentEventsByPersistenceId" in {
      val probe = TestProbe()
      writeJournalUtils.writeMessages(0, 9, "a-1", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
      writeJournalUtils.writeMessages(0, 9, "a-2", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
      val events =
        readJournal.currentEventsByPersistenceId("a-2", 0L, Long.MaxValue).runWith(Sink.seq).futureValue
      println(events)
      events.forall(_.persistenceId == "a-2") shouldBe true
    }
  }

}
