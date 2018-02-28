package com.github.j5ik2o.ak.kcl.dsl

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.github.j5ik2o.ak.aws.{ AwsClientConfig, AwsKinesisClient }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, FreeSpecLike, Matchers }

import scala.util.Try

class KCLSourceSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with FreeSpecLike
    with BeforeAndAfterAll
    with PropertyChecks
    with Matchers
    with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))

  val region              = Regions.AP_NORTHEAST_1
  val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance

  val awsKinesisClient: AwsKinesisClient = new AwsKinesisClient(AwsClientConfig(region))
  val awsDynamoDBClient: AmazonDynamoDBAsync = AmazonDynamoDBAsyncClientBuilder
    .standard()
    .withRegion(region)
    .withCredentials(credentialsProvider)
    .build()

  val applicationName = "kcl-source-spec"
  val streamName      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  val kinesisClientLibConfiguration =
    new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
      .withTableName(streamName)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

  import system.dispatcher

  implicit val mat = ActorMaterializer()

  override protected def beforeAll(): Unit = {
    awsKinesisClient.createStream(streamName, 1)
    awsKinesisClient.waitStreamToCreated(streamName)
  }

  override protected def afterAll(): Unit = {
    awsKinesisClient.deleteStream(streamName)
    awsDynamoDBClient.deleteTable(streamName)
  }

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      val future = KCLSource(
        kinesisClientLibConfiguration = kinesisClientLibConfiguration,
        kinesisClient = Some(awsKinesisClient.underlying),
        dynamoDBClient = Some(awsDynamoDBClient),
        metricsFactory = Some(new NullMetricsFactory)
      ).runWith(Sink.foreach(println))

      TimeUnit.MINUTES.sleep(1)

      val text = "abc"
      awsKinesisClient.putRecord(
        new PutRecordRequest()
          .withStreamName(streamName)
          .withPartitionKey("k-1")
          .withData(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))
      )

      val result = future.futureValue
      println(result)
    }
  }

}
