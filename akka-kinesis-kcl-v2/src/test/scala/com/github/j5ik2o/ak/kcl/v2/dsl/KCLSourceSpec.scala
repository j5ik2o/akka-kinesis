package com.github.j5ik2o.ak.kcl.v2.dsl

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.stage.AsyncCallback
import akka.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.dimafeng.testcontainers.{ Container, ForAllTestContainer, LocalStackContainer }
import com.github.j5ik2o.ak.kcl.v2.stage.KCLSourceStage
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  CreateStreamRequest,
  DescribeStreamRequest,
  PutRecordRequest,
  StreamStatus
}
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.metrics.NullMetricsFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.net.{ InetAddress, URI }
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

class KCLSourceSpec
    extends TestKit(ActorSystem("KCLSourceSpec"))
    with AnyFreeSpecLike
    with Matchers
    with ScalaFutures
    with ForAllTestContainer
    with Eventually {
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))

  val localStack: LocalStackContainer = LocalStackContainer(
    tag = "0.12.10",
    services = Seq(
      JavaLocalStackContainer.Service.DYNAMODB,
      JavaLocalStackContainer.Service.KINESIS,
      JavaLocalStackContainer.Service.CLOUDWATCH
    )
  )

  val applicationName: String = "kcl-source-spec"
  val streamName: String      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val schedulerId: String     = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  override def container: Container = localStack

  var dynamodbAsyncClient: DynamoDbAsyncClient     = _
  var kinesisAsyncClient: KinesisAsyncClient       = _
  var cloudwatchAsyncClient: CloudWatchAsyncClient = _

  override def afterStart(): Unit = {
    val credentialsProvider             = localStack.defaultCredentialsProvider
    val dynamoDbEndpointConfiguration   = localStack.endpointConfiguration(JavaLocalStackContainer.Service.DYNAMODB)
    val kinesisEndpointConfiguration    = localStack.endpointConfiguration(JavaLocalStackContainer.Service.KINESIS)
    val cloudwatchEndpointConfiguration = localStack.endpointConfiguration(JavaLocalStackContainer.Service.CLOUDWATCH)

    dynamodbAsyncClient = DynamoDbAsyncClient
      .builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            credentialsProvider.getCredentials.getAWSAccessKeyId,
            credentialsProvider.getCredentials.getAWSSecretKey
          )
        )
      )
      .endpointOverride(URI.create(dynamoDbEndpointConfiguration.getServiceEndpoint))
      .httpClient(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1).build())
      .build()

    kinesisAsyncClient = KinesisAsyncClient
      .builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            credentialsProvider.getCredentials.getAWSAccessKeyId,
            credentialsProvider.getCredentials.getAWSSecretKey
          )
        )
      )
      .endpointOverride(URI.create(kinesisEndpointConfiguration.getServiceEndpoint))
      .httpClient(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1).build())
      .build()

    cloudwatchAsyncClient = CloudWatchAsyncClient
      .builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            credentialsProvider.getCredentials.getAWSAccessKeyId,
            credentialsProvider.getCredentials.getAWSSecretKey
          )
        )
      )
      .endpointOverride(URI.create(cloudwatchEndpointConfiguration.getServiceEndpoint))
      .httpClient(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1).build())
      .build()

    val createStreamResult = kinesisAsyncClient
      .createStream(CreateStreamRequest.builder().streamName(streamName).shardCount(1).build()).get()
    assert(createStreamResult.sdkHttpResponse().isSuccessful)
    waitStreamToCreated(streamName)
  }

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1.seconds): Try[Unit] = {
    @tailrec
    def go: Try[Unit] = {
      Try { kinesisAsyncClient.describeStream(DescribeStreamRequest.builder().streamName(streamName).build()).get() } match {
        case Success(result) if result.streamDescription.streamStatus == StreamStatus.ACTIVE =>
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
    Try { kinesisAsyncClient.listStreams().get() }.flatMap { result =>
      result.streamNames.asScala.foldLeft(Try(())) {
        case (_, streamName) =>
          waitStreamToCreated(streamName)
      }
    }
  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "should be able to consume message" in {

      val ec                          = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
      var result: KinesisClientRecord = null
      val (sw, future) = KCLSource
        .ofCustomSchedulerWithoutCheckpoint(
          1.seconds, {
            (
                onInitializeCallback: AsyncCallback[InitializationInput],
                onProcessRecordsInputCallback: AsyncCallback[ProcessRecordsInput],
                onLeaseLostInputCallback: AsyncCallback[LeaseLostInput],
                onShardEndedInputCallback: AsyncCallback[ShardEndedInput],
                onShutdownRequestedInputCallback: AsyncCallback[ShutdownRequestedInput]
            ) =>
              val configBuilder = KCLSourceStage.newConfigBuilderF(
                streamName = streamName,
                applicationName = applicationName,
                schedulerIdentifier = schedulerId,
                kinesisClient = kinesisAsyncClient,
                dynamodbClient = dynamodbAsyncClient,
                cloudWatchClient = cloudwatchAsyncClient,
                recordProcessorFactoryOpt = None
              )(
                onInitializeCallback,
                onProcessRecordsInputCallback,
                onLeaseLostInputCallback,
                onShardEndedInputCallback,
                onShutdownRequestedInputCallback
              )
              KCLSourceStage.newScheduler(
                configBuilder.checkpointConfig,
                configBuilder.coordinatorConfig,
                configBuilder.leaseManagementConfig.initialPositionInStream(
                  InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
                ),
                configBuilder.lifecycleConfig,
                configBuilder.metricsConfig().metricsFactory(new NullMetricsFactory()),
                configBuilder.processorConfig,
                configBuilder
                  .retrievalConfig()
                  .retrievalSpecificConfig(new PollingConfig(streamName, kinesisAsyncClient))
                  .initialPositionInStreamExtended(
                    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
                  )
              )
          }
        )(ec)
        .viaMat(KillSwitches.single)(Keep.right)
        .map { msg =>
          println(s">>>>>>> msg = $msg")
          result = msg.record
          msg
        }
        .via(KCLFlow.ofCheckpoint())
        .toMat(Sink.head)(Keep.both)
        .run()

      Thread.sleep(1000 * 30)

      val text = "abc"
      for { i <- 1 to 100 } {
        val putRecordResult = kinesisAsyncClient
          .putRecord(
            PutRecordRequest
              .builder()
              .streamName(streamName)
              .partitionKey(s"k-$i")
              .data(SdkBytes.fromString(text, StandardCharsets.UTF_8)).build()
          ).get()
        println("putRecordResult = " + putRecordResult.toString)
      }

      Thread.sleep(1000 * 30)

      eventually {
        assert(result != null && new String(result.data().array()) == text)
      }

      sw.shutdown()
      future.futureValue

    }
  }
}
