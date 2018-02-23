package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ BeforeAndAfterAll, FreeSpecLike, Matchers }

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with FreeSpecLike
    with BeforeAndAfterAll
    with PropertyChecks
    with Matchers {

  val region     = Regions.AP_NORTHEAST_1
  val client     = new AwsKinesisClient(AwsClientConfig(region))
  val streamName = sys.env("KPL_STREAM_NAME")

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
      val probe = Source
        .single(
          new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(ByteBuffer.wrap(data.getBytes()))
        )
        .via(KPLFlow(streamName, kinesisProducerConfiguration, kplFlowSettings))
        .runWith(TestSink.probe)

      val result = probe.request(1).expectNext()
      result should not be null
      result.getShardId should not be null
      result.getSequenceNumber should not be null
      result.getErrorCode shouldBe null
      result.getErrorMessage shouldBe null
    }
  }

}
