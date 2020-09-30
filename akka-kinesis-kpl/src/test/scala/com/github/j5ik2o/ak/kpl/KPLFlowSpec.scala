package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with Matchers {

  val region = Regions.AP_NORTHEAST_1

  val client = AmazonKinesisClient.builder().withRegion(region).build()

  val streamName = sys.env("KPL_STREAM_NAME") + UUID.randomUUID().toString

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1 seconds): Try[Unit] = {
    def go: Try[Unit] = {
      Try { client.describeStream(streamName) } match {
        case Success(result) if result.getStreamDescription.getStreamStatus == "ACTIVE" =>
          println(s"waiting completed: $streamName, $result")
          Success(())
        case Failure(ex: ResourceNotFoundException) =>
          Thread.sleep(waitDuration.toMillis)
          println("waiting until stream creates")
          go
        case Failure(ex) => Failure(ex)
        case _ =>
          Thread.sleep(waitDuration.toMillis)
          println("waiting until stream creates")
          go

      }
    }
    val result = go
    Thread.sleep(waitDuration.toMillis)
    result
  }

  def waitStreamsToCreated(): Try[Unit] = {
    Try { client.listStreams() }.flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(())) {
        case (_, streamName) =>
          waitStreamToCreated(streamName)
      }
    }
  }

  override protected def beforeAll(): Unit = {
    client.createStream(streamName, 1)
    waitStreamToCreated(streamName)
  }

  override protected def afterAll(): Unit = {
    client.deleteStream(streamName)
  }

  "KPLFlow" - {
    "publisher" in {
      implicit val ec  = system.dispatcher
      implicit val mat = ActorMaterializer()

      val partitionKey = "123"
      val data         = "XYZ"

      val kinesisProducerConfiguration = new KinesisProducerConfiguration()
        .setRegion(region.getName)
        .setCredentialsRefreshDelay(100)

      val kplFlowSettings = KPLFlowSettings.byNumberOfShards(1)
      val probe: TestSubscriber.Probe[UserRecordResult] = Source
        .single(
          new UserRecord()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()))
        )
        .viaMat(KPLFlow(streamName, kinesisProducerConfiguration, kplFlowSettings))(Keep.left)
        .runWith(TestSink.probe)

      val result = probe.request(1).expectNext()
      result should not be null
      result.getShardId should not be null
      result.getSequenceNumber should not be null
      result.getAttempts.asScala.forall(_.isSuccessful) shouldBe true
    }
  }

}
