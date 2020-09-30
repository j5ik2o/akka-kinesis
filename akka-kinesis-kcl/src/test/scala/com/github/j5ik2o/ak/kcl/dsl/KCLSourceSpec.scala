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
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, ResourceNotFoundException }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClient }
import com.dimafeng.testcontainers.{
  Container,
  FixedHostPortGenericContainer,
  ForAllTestContainer,
  GenericContainer,
  LocalStackContainer,
  MultipleContainers
}
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }
import org.testcontainers.containers.wait.strategy.Wait

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
  System.setProperty("com.amazonaws.sdk.disableCbor", "1");
  protected val dynamoDBImageVersion = "1.13.2"

  protected val dynamoDBImageName = s"amazon/dynamodb-local:$dynamoDBImageVersion"

  val applicationName = "kcl-source-spec"
  val streamName      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  val kinesisLocal = GenericContainer(
    "vsouza/kinesis-local",
    exposedPorts = Seq(4567),
    waitStrategy = Wait.forListeningPort(),
    command = Seq("--port", "4567", "--createStreamMs", "5")
  )

  val dynamoDbLocal = GenericContainer(
    dynamoDBImageName,
    exposedPorts = Seq(8000),
    waitStrategy = Wait.forListeningPort(),
    command = Seq("-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb")
  )

  override def container: Container = MultipleContainers(kinesisLocal, dynamoDbLocal)

  var awsKinesisClient: AmazonKinesis                              = _
  var awsDynamoDBClient: AmazonDynamoDBAsync                       = _
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = _

  override def afterStart(): Unit = {
    awsDynamoDBClient = AmazonDynamoDBAsyncClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
      .withEndpointConfiguration(
        new EndpointConfiguration(
          s"http://127.0.0.1:${dynamoDbLocal.container.getFirstMappedPort}",
          Regions.AP_NORTHEAST_1.getName
        )
      )
      .build()

    val kinesisEndpoint = s"http://127.0.0.1:${kinesisLocal.container.getFirstMappedPort}"
    println(kinesisEndpoint)
    awsKinesisClient = AmazonKinesisClient
      .builder()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
      .withEndpointConfiguration(
        new EndpointConfiguration(
          kinesisEndpoint,
          Regions.AP_NORTHEAST_1.getName
        )
      )
      .build()

    val result = awsKinesisClient.createStream(streamName, 1)
    println(result)
    waitStreamToCreated(streamName)

    kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      applicationName,
      streamName,
      new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")),
      workerId
    ).withTableName(streamName)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

  }

  override def beforeStop(): Unit = {
//    awsKinesisClient.deleteStream(streamName)
    //   awsDynamoDBClient.deleteTable(streamName)
  }

  implicit val defaultPatience = PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))

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

  implicit val mat = ActorMaterializer()

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      val future = KCLSource(
        kinesisClientLibConfiguration = kinesisClientLibConfiguration,
        kinesisClient = Some(awsKinesisClient),
        dynamoDBClient = Some(awsDynamoDBClient),
        metricsFactory = Some(new NullMetricsFactory)
      ).runWith(Sink.foreach(println))

      val text = "abc"
      awsKinesisClient.putRecord(
        new PutRecordRequest()
          .withStreamName(streamName)
          .withPartitionKey("k-1")
          .withData(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))
      )

      TimeUnit.SECONDS.sleep(10)

    }
  }

}
