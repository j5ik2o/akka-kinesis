package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord }
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ BeforeAndAfterAll, FreeSpecLike, Matchers }

import scala.concurrent.duration._

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with FreeSpecLike
    with BeforeAndAfterAll
    with PropertyChecks
    with Matchers {

  val region     = Regions.US_EAST_1
  val client     = AmazonKinesisClientBuilder.standard().withRegion(region).build()
  val streamName = sys.env("KPL_STREAM_NAME")

  override protected def beforeAll(): Unit = {
    client.createStream(streamName, 1)
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

      val kplFlowSettings = KPLFLowSettings(maxRetries = 10,
                                            backoffStrategy = RetryBackoffStrategy.Exponential,
                                            retryInitialTimeout = 200 millis)
      val probe = Source
        .single(
          new UserRecord()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)))
        )
        .via(KPLFlow(kinesisProducerConfiguration, kplFlowSettings))
        .runWith(TestSink.probe)

      probe.request(1).expectNext().getAttempts.size() shouldBe 1
    }
  }

}
