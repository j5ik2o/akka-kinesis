package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer
import java.util.UUID
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, KillSwitches, Materializer }
import akka.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.dimafeng.testcontainers.LocalStackContainer
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }
import org.testcontainers.utility.DockerImageName

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with Eventually {

  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
  System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true")

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = Span(60 * sys.env.getOrElse("TEST_TIME_FACTOR", "2").toInt, Seconds),
      interval = Span(500 * sys.env.getOrElse("TEST_TIME_FACTOR", "2").toInt, Millis)
    )

  val services: Seq[JavaLocalStackContainer.Service] =
    Seq(JavaLocalStackContainer.Service.KINESIS, JavaLocalStackContainer.Service.CLOUDWATCH)

  val localStack: LocalStackContainer = LocalStackContainer(
    dockerImageName = DockerImageName.parse("localstack/localstack:2.1.0"),
    services = Seq(
      JavaLocalStackContainer.Service.KINESIS,
      JavaLocalStackContainer.Service.CLOUDWATCH
    )
  ).configure { c =>
    val r = services.map(_.getPort)
    c
      .withExposedPorts(JavaLocalStackContainer.Service.KINESIS.getPort, r(1))
      .withEnv(Map("AWS_CBOR_DISABLE" -> "true", "USE_SSL" -> "true").asJava)
    c
  }

  var awsKinesisClient: AmazonKinesis                            = _
  var kinesisProducerConfiguration: KinesisProducerConfiguration = _

  val streamName: String = sys.env.getOrElse("STREAM_NAME", "kpl-flow-spec") + UUID.randomUUID().toString

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

  def afterStart(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(localStack.container.getAccessKey, localStack.container.getSecretKey)
    )
    val region         = localStack.container.getRegion
    val kinesisHost    = localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.KINESIS).getHost
    val kinesisPort    = localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.KINESIS).getPort
    val cloudwatchHost = localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.CLOUDWATCH).getHost
    val cloudwatchPort = localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.CLOUDWATCH).getPort

    println(s"kinesis = $kinesisHost:$kinesisPort, cloudwatch = $cloudwatchHost:$cloudwatchPort")

    awsKinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(
        new EndpointConfiguration(
          localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.KINESIS).toString,
          region
        )
      )
      .build()

    awsKinesisClient.createStream(streamName, 1)
    waitStreamToCreated(streamName)

    kinesisProducerConfiguration = new KinesisProducerConfiguration()
      .setLogLevel("debug")
      .setRegion(region)
      .setCredentialsProvider(credentialsProvider)
      .setMetricsCredentialsProvider(credentialsProvider)
      .setCloudwatchEndpoint(kinesisHost)
      .setCloudwatchPort(kinesisPort.toLong)
      .setKinesisEndpoint(cloudwatchHost)
      .setKinesisPort(kinesisPort.toLong)
      .setCredentialsRefreshDelay(100 * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
      .setVerifyCertificate(false)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    localStack.start()
    afterStart()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    localStack.stop()
  }

  "KPLFlow" - {
    "publisher" in {
      implicit val ec: ExecutionContext = system.dispatcher
      implicit val mat: Materializer    = ActorMaterializer()

      var result: UserRecordResult = null
      val partitionKey             = "123"
      val data                     = "XYZ"

      val kplFlowSettings = KPLFlowSettings.byNumberOfShards(1)

      val ((q, sw), future) = Source
        .queue[UserRecord](5)
        .viaMat(KillSwitches.single)(Keep.both)
        .viaMat(KPLFlow(kinesisProducerConfiguration, kplFlowSettings))(Keep.left)
        .toMat(Sink.foreach(msg => result = msg))(Keep.both)
        .run()

      q.offer(new UserRecord(streamName, partitionKey, ByteBuffer.wrap(data.getBytes())))

      eventually(Timeout(30.seconds)) {
        assert(result != null)
      }

      sw.shutdown()
      future.futureValue
    }
  }

}
