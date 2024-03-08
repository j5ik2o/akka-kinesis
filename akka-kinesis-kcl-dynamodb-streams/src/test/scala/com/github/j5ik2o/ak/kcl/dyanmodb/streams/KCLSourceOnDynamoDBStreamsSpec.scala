package com.github.j5ik2o.ak.kcl.dyanmodb.streams

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{ Keep, Sink }
import akka.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClient }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClient,
  AmazonDynamoDBStreams,
  AmazonDynamoDBStreamsClient
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.dimafeng.testcontainers._
import com.github.j5ik2o.ak.kcl.dsl.{ KCLFlow, RandomPortUtil }
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.localstack.{ LocalStackContainer => JavaLocalStackContainer }
import org.testcontainers.containers.wait.strategy.Wait

import java.net.InetAddress
import java.util
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.JavaConverters._

class KCLSourceOnDynamoDBStreamsSpec
    extends TestKit(ActorSystem("KCLSourceInDynamoDBStreamsSpec"))
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

  protected val dynamoDBImageVersion = "1.13.2"

  protected val dynamoDBImageName: String = s"amazon/dynamodb-local:$dynamoDBImageVersion"

  protected val dynamoDBPort: Int = RandomPortUtil.temporaryServerPort()

  val host: String = DockerClientFactory.instance().dockerHostIpAddress()

  protected val dynamoDBEndpoint: String = s"http://$host:$dynamoDBPort"

  protected val dynamoDbLocalContainer: FixedHostPortGenericContainer = FixedHostPortGenericContainer(
    dynamoDBImageName,
    exposedHostPort = dynamoDBPort,
    exposedContainerPort = 8000,
    command = Seq("-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb"),
    waitStrategy = Wait.forListeningPort()
  )

  private val localStack: LocalStackContainer = LocalStackContainer(
    services = Seq(
      JavaLocalStackContainer.Service.CLOUDWATCH
    )
  )

  val applicationName: String = "kcl-source-spec"
  val workerId: String        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  val container: Container = MultipleContainers(dynamoDbLocalContainer, localStack)

  var awsDynamoDB: AmazonDynamoDB            = _
  var dynamoDBStreams: AmazonDynamoDBStreams = _
  var awsCloudWatch: AmazonCloudWatch        = _
  val tableName: String                      = "test-" + UUID.randomUUID().toString

  def afterStart(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(localStack.container.getAccessKey, localStack.container.getSecretKey)
    )
    val dynamoDbEndpointConfiguration = new EndpointConfiguration(dynamoDBEndpoint, Regions.AP_NORTHEAST_1.getName)
    val cloudwatchEndpointConfiguration = new EndpointConfiguration(
      localStack.container.getEndpointOverride(JavaLocalStackContainer.Service.CLOUDWATCH).toString,
      localStack.container.getRegion
    )

    awsDynamoDB = AmazonDynamoDBClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    dynamoDBStreams = AmazonDynamoDBStreamsClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    awsCloudWatch = AmazonCloudWatchClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(cloudwatchEndpointConfiguration)
      .build()

  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    afterStart()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  import system.dispatcher

  "KCLSourceOnDynamoDBStreamsSpec" - {
    "dynamodb streams" in {
      val attributeDefinitions = new util.ArrayList[AttributeDefinition]
      attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"))

      val keySchema = new util.ArrayList[KeySchemaElement]
      keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))

      val request = new CreateTableRequest()
        .withTableName(tableName)
        .withKeySchema(keySchema)
        .withAttributeDefinitions(attributeDefinitions)
        .withProvisionedThroughput(
          new ProvisionedThroughput()
            .withReadCapacityUnits(10L)
            .withWriteCapacityUnits(10L)
        ).withStreamSpecification(
          new StreamSpecification()
            .withStreamViewType(StreamViewType.NEW_IMAGE)
            .withStreamEnabled(true)
        )
      val table = awsDynamoDB.createTable(request)

      while (!awsDynamoDB.listTables(1).getTableNames.asScala.contains(tableName)) {
        println("waiting for create table...")
        Thread.sleep(1000)
      }

      val streamArn = table.getTableDescription.getLatestStreamArn
      val credentialsProvider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(localStack.container.getAccessKey, localStack.container.getSecretKey)
      )

      val kinesisClientLibConfiguration = KCLConfiguration.fromConfig(
        system.settings.config,
        applicationName,
        UUID.randomUUID(),
        streamArn,
        credentialsProvider,
        credentialsProvider,
        credentialsProvider,
        configOverrides = Some(
          KCLConfiguration.ConfigOverrides(positionInStreamOpt = Some(InitialPositionInStream.TRIM_HORIZON))
        )
      )

      val adapterClient: AmazonDynamoDBStreamsAdapterClient =
        new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams)

      val executorService = Executors.newCachedThreadPool()
      var result: String  = null
      val (sw, future) =
        KCLSourceOnDynamoDBStreams
          .withoutCheckpoint(
            kinesisClientLibConfiguration,
            adapterClient,
            awsDynamoDB,
            Some(awsCloudWatch),
            None,
            executorService
          )
          .viaMat(KillSwitches.single)(Keep.right)
          .map { msg =>
            val recordAdaptor = msg.record
              .asInstanceOf[RecordAdapter]
            val streamRecord = recordAdaptor.getInternalObject.getDynamodb
            val newImage     = streamRecord.getNewImage.asScala
            val id           = newImage("Id").getN
            val message      = newImage("Value").getS
            println(s"id = $id, message = $message")
            result = message
            msg
          }
          .via(KCLFlow.ofCheckpoint())
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val text = "abc"
      awsDynamoDB.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(Map("Id" -> new AttributeValue().withN("1"), "Value" -> new AttributeValue().withS(text)).asJava)
      )

      eventually(Timeout(Span.Max)) {
        assert(result != null && result == text)
      }

      sw.shutdown()
      future.futureValue
    }
  }

}
