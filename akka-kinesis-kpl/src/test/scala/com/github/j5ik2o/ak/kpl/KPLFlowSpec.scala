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
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ BeforeAndAfterAll, FreeSpecLike, Matchers }

import scala.collection.JavaConverters._

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with FreeSpecLike
    with BeforeAndAfterAll
    with PropertyChecks
    with Matchers {

  val region     = Regions.AP_NORTHEAST_1
  val client     = new AwsKinesisClient(AwsClientConfig(region))
  val streamName = sys.env("KPL_STREAM_NAME") + UUID.randomUUID().toString

  override protected def beforeAll(): Unit = {
    client.createStream(streamName, 1)
    client.waitStreamToCreated(streamName)
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
