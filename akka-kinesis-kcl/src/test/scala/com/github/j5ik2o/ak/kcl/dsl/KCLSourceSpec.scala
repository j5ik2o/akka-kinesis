package com.github.j5ik2o.ak.kcl.dsl

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{ Keep, Sink }
import akka.testkit.TestKit
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClientBuilder }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, Record, ResourceNotFoundException }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.amazonaws.SDKGlobalConfiguration
import com.dimafeng.testcontainers.{ Container, LocalStackContainer }
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KCLSourceSpec
    extends TestKit(ActorSystem("KCLSourceSpec"))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with Eventually {
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = Span(60 * sys.env.getOrElse("TEST_TIME_FACTOR", "2").toInt, Seconds),
      interval = Span(500 * sys.env.getOrElse("TEST_TIME_FACTOR", "2").toInt, Millis)
    )

  val localStack: LocalStackContainer = LocalStackContainer(
    tag = "0.9.5",
    services = Seq(
      JavaLocalStackContainer.Service.DYNAMODB,
      JavaLocalStackContainer.Service.KINESIS,
      JavaLocalStackContainer.Service.CLOUDWATCH
    )
  )

  val applicationName: String = "kcl-source-spec"
  val streamName: String      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId: String        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  def container: Container = localStack

  var awsKinesisClient: AmazonKinesis                              = _
  var awsDynamoDBClient: AmazonDynamoDBAsync                       = _
  var awsCloudWatch: AmazonCloudWatch                              = _
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = _

  def afterStart(): Unit = {
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

    val httpStatusCode = awsKinesisClient.createStream(streamName, 1).getSdkHttpMetadata.getHttpStatusCode
    assert(httpStatusCode == 200)
    waitStreamToCreated(streamName)

    kinesisClientLibConfiguration = KCLConfiguration.fromConfig(
      system.settings.config,
      applicationName,
      UUID.randomUUID(),
      streamName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider,
      configOverrides = Some(
        KCLConfiguration.ConfigOverrides(positionInStreamOpt = Some(InitialPositionInStream.TRIM_HORIZON))
      )
    )

  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    afterStart()
  }

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1.seconds): Try[Unit] = {
    @tailrec
    def go: Try[Unit] = {
      Try { awsKinesisClient.describeStream(streamName) } match {
        case Success(result) if result.getStreamDescription.getStreamStatus == "ACTIVE" =>
          println(s"waiting completed: $streamName, $result")
          Success(())
        case Failure(_: ResourceNotFoundException) =>
          Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
          println("waiting until stream creates")
          go
        case Failure(ex) => Failure(ex)
        case _ =>
          Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
          println("waiting until stream creates")
          go

      }
    }
    val result = go
    Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
    result
  }

  def waitStreamsToCreated(): Try[Unit] = {
    Try { awsKinesisClient.listStreams() }.flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(())) { case (_, streamName) =>
        waitStreamToCreated(streamName)
      }
    }
  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      var result: Record = null
      val (sw, future) = KCLSource
        .withoutCheckpoint(
          kinesisClientLibConfiguration,
          amazonKinesisOpt = Some(awsKinesisClient),
          amazonDynamoDBOpt = Some(awsDynamoDBClient),
          amazonCloudWatchOpt = Some(awsCloudWatch),
          iMetricsFactoryOpt = Some(new NullMetricsFactory)
        ).viaMat(KillSwitches.single)(Keep.right)
        .map { msg =>
          result = msg.record
          msg
        }
        .via(KCLFlow.ofCheckpoint())
        .toMat(Sink.ignore)(Keep.both)
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
