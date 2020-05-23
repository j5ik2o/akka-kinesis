package com.github.j5ik2o.ak.persistence

import java.net.InetAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PluginSpec
import akka.persistence.journal.{ JournalSpec, WriteJournalUtils }
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink }
import akka.testkit.{ ImplicitSender, TestProbe }
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }

class KinesisScalaReadJournalSpec
    extends PluginSpec(ConfigFactory.load("journal-spec.conf"))
    with ImplicitSender
    with Matchers
    with ScalaFutures {
  val streamName = sys.env("STREAM_NAME") + UUID.randomUUID().toString
  val region     = Regions.AP_NORTHEAST_1

  val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance

  val awsKinesisClient = new AwsKinesisClient(AwsClientConfig(region))
  val awsDynamoDBClient: AmazonDynamoDBAsync = AmazonDynamoDBAsyncClientBuilder
    .standard()
    .withRegion(region)
    .withCredentials(credentialsProvider)
    .build()

  implicit val mat                         = ActorMaterializer()
  implicit lazy val system: ActorSystem    = ActorSystem("KinesisReadJournalSpec", config.withFallback(JournalSpec.config))
  var writeJournalUtils: WriteJournalUtils = _
  var readJournal: KinesisScalaReadJournal = _

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(500, Millis))

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    awsKinesisClient.createStream(streamName, 1)
    awsKinesisClient.waitStreamToCreated(streamName)
  }

  protected override def afterAll(): Unit = {
    awsKinesisClient.deleteStream(streamName)
    awsDynamoDBClient.deleteTable(streamName)
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
//    "currentPersistenceIds" in {
//      val probe = TestProbe()
//      writeJournalUtils.writeMessages(0, 9, "a-1", probe.ref, writerUuid)
//      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
//      val persistenceIds = readJournal.currentPersistenceIds().runWith(Sink.seq).futureValue
//      persistenceIds shouldBe Seq("a-1")
//    }
//    "currentEventsByPersistenceId" in {
//      val probe = TestProbe()
//      writeJournalUtils.writeMessages(0, 9, "a-1", probe.ref, writerUuid)
//      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
//      writeJournalUtils.writeMessages(0, 9, "a-2", probe.ref, writerUuid)
//      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
//      val events =
//        readJournal.currentEventsByPersistenceId("a-2", 0L, Long.MaxValue).runWith(Sink.seq).futureValue
//      println(s"events = $events")
//      events.forall(_.persistenceId == "a-2") shouldBe true
//    }
    "kcl" in {
      val applicationName = "akka-kinesis"
      val workerId        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()
      val config = new KinesisClientLibConfiguration(
        applicationName,
        streamName,
        DefaultAWSCredentialsProviderChain.getInstance,
        workerId
      ).withTableName(streamName).withInitialPositionInStream(InitialPositionInStream.LATEST)

      val (workerF, eventsF) = readJournal.eventsByKCL(config).take(2).toMat(Sink.seq)(Keep.both).run()

      val probe = TestProbe()
      writeJournalUtils.writeMessages(0, 9, "a-1", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
      writeJournalUtils.writeMessages(0, 9, "a-2", probe.ref, writerUuid)
      writeJournalUtils.expectWriteMessagesSuccessful(probe, 10)
      println(eventsF.futureValue)
    }
  }

}
