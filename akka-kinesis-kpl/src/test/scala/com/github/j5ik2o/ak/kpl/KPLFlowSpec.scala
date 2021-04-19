package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, KillSwitches }
import akka.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.dimafeng.testcontainers.{ Container, ForAllTestContainer, GenericContainer }
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }
import org.testcontainers.containers.wait.strategy.Wait

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ForAllTestContainer
    with Eventually {

  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
  System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true")

  val services: Seq[JavaLocalStackContainer.Service] =
    Seq(JavaLocalStackContainer.Service.KINESIS, JavaLocalStackContainer.Service.CLOUDWATCH)

  val localStack: GenericContainer = new GenericContainer(
    dockerImage = "localstack/localstack:0.9.5",
    exposedPorts = services.map(_.getPort),
    env = Map(
      "SERVICES"         -> services.map(_.getLocalStackName).mkString(","),
      "USE_SSL"          -> "true",
      "AWS_CBOR_DISABLE" -> "true"
    ),
    command = Seq(),
    classpathResourceMapping = Seq(),
    waitStrategy = Some(Wait.forLogMessage(".*Ready\\.\n", 1))
  )

  override def container: Container = localStack

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
      result.getStreamNames.asScala.foldLeft(Try(())) { case (_, streamName) =>
        waitStreamToCreated(streamName)
      }
    }
  }

  override def afterStart(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))
    val host                = DockerClientFactory.instance().dockerHostIpAddress()
    val kinesisPort         = localStack.container.getMappedPort(JavaLocalStackContainer.Service.KINESIS.getPort)
    val cloudwatchPort      = localStack.container.getMappedPort(JavaLocalStackContainer.Service.CLOUDWATCH.getPort)

    println(s"kinesis = $kinesisPort, cloudwatch = $cloudwatchPort")

    awsKinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(
        new EndpointConfiguration(s"https://$host:$kinesisPort", Regions.AP_NORTHEAST_1.getName)
      )
      .build()

    awsKinesisClient.createStream(streamName, 1)
    waitStreamToCreated(streamName)

    kinesisProducerConfiguration = new KinesisProducerConfiguration()
      .setCredentialsProvider(credentialsProvider)
      .setRegion(Regions.AP_NORTHEAST_1.getName)
      .setKinesisEndpoint(host)
      .setKinesisPort(kinesisPort.toLong)
      .setCloudwatchEndpoint(host)
      .setCloudwatchPort(cloudwatchPort.toLong)
      .setCredentialsRefreshDelay(100)
      .setVerifyCertificate(false)
  }

  override def beforeStop(): Unit = {
    awsKinesisClient.deleteStream(streamName)
  }

  "KPLFlow" - {
    "publisher" in {
      implicit val ec  = system.dispatcher
      implicit val mat = ActorMaterializer()

      var result: UserRecordResult = null
      val partitionKey             = "123"
      val data                     = "XYZ"

      val kplFlowSettings = KPLFlowSettings.byNumberOfShards(1)
      val (sw, future) = Source
        .single(
          new UserRecord()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()))
        )
        .viaMat(KillSwitches.single)(Keep.right)
        .viaMat(KPLFlow(streamName, kinesisProducerConfiguration, kplFlowSettings))(Keep.left)
        .toMat(Sink.foreach { msg => result = msg })(Keep.both)
        .run()

      TimeUnit.SECONDS.sleep(10)

      sw.shutdown()
      future.futureValue
      result should not be null
      result.getShardId should not be null
      result.getSequenceNumber should not be null
      result.getAttempts.asScala.forall(_.isSuccessful) shouldBe true
    }
  }

}
