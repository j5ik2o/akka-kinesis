package com.github.j5ik2o.ak.kcl.dsl

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.{ ActorMaterializer, KillSwitches }
import akka.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClientBuilder }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, Record, ResourceNotFoundException }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.dimafeng.testcontainers.{ Container, ForAllTestContainer, LocalStackContainer }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KCLSourceSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with AnyFreeSpecLike
    with Matchers
    with ScalaFutures
    with ForAllTestContainer
    with Eventually {
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")

  implicit val defaultPatience = PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))

  val localStack = LocalStackContainer(
    tag = "0.9.5",
    services = Seq(
      JavaLocalStackContainer.Service.DYNAMODB,
      JavaLocalStackContainer.Service.KINESIS,
      JavaLocalStackContainer.Service.CLOUDWATCH
    )
  )

  val applicationName = "kcl-source-spec"
  val streamName      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  override def container: Container = localStack

  var awsKinesisClient: AmazonKinesis                              = _
  var awsDynamoDBClient: AmazonDynamoDBAsync                       = _
  var awsCloudWatch: AmazonCloudWatch                              = _
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = _

  override def afterStart(): Unit = {
    val credentialsProvider             = localStack.defaultCredentialsProvider
    val dynamoDbEndpointConfiguration   = localStack.endpointConfiguration(JavaLocalStackContainer.Service.DYNAMODB)
    val kinesisEndpointConfiguration    = localStack.endpointConfiguration(JavaLocalStackContainer.Service.KINESIS)
    val cloudwatchEndpointConfiguration = localStack.endpointConfiguration(JavaLocalStackContainer.Service.CLOUDWATCH)

    awsDynamoDBClient = AmazonDynamoDBAsyncClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    awsKinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(kinesisEndpointConfiguration)
      .build()

    awsCloudWatch = AmazonCloudWatchClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(cloudwatchEndpointConfiguration).build()

    assert(awsKinesisClient.createStream(streamName, 1).getSdkHttpMetadata.getHttpStatusCode == 200)
    waitStreamToCreated(streamName)

    kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      applicationName,
      streamName,
      credentialsProvider,
      workerId
    ).withTableName(streamName)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

  }

  override def beforeStop(): Unit = {
    awsKinesisClient.deleteStream(streamName)
    awsDynamoDBClient.deleteTable(streamName)
  }

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1 seconds): Try[Unit] = {
    def go: Try[Unit] = {
      Try { awsKinesisClient.describeStream(streamName) } match {
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
    Try { awsKinesisClient.listStreams() }.flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(())) {
        case (_, streamName) =>
          waitStreamToCreated(streamName)
      }
    }
  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      var result: Record = null
      val (sw, future) = KCLSource(
        kinesisClientLibConfiguration = kinesisClientLibConfiguration,
        kinesisClient = Some(awsKinesisClient),
        dynamoDBClient = Some(awsDynamoDBClient),
        cloudWatchClient = Some(awsCloudWatch),
        metricsFactory = Some(new NullMetricsFactory)
      ).viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach { msg: Record => result = msg })(Keep.both)
        .run()

      val text = "abc"
      awsKinesisClient.putRecord(
        new PutRecordRequest()
          .withStreamName(streamName)
          .withPartitionKey("k-1")
          .withData(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))
      )

      eventually {
        assert(result != null && new String(result.getData.array()) == text)
      }

      sw.shutdown()
      future.futureValue

    }
  }

}
