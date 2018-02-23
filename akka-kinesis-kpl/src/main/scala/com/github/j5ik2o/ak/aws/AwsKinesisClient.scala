package com.github.j5ik2o.ak.aws

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{ AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder }
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object StreamStatus extends Enumeration {
  val Created, Deleted = Value
}

class AwsKinesisClient(awsClientConfig: AwsClientConfig) extends AwsKinesis with AwsKinesisAsync {

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val amazonKinesisAsyncClientBuilder =
    AmazonKinesisAsyncClientBuilder.standard()

  awsClientConfig.credentialsProvider.foreach { v =>
    logger.info(s"awsAccessKeyId = ${v.getCredentials.getAWSAccessKeyId}")
    logger.info(s"awsSecretKey = ${v.getCredentials.getAWSSecretKey}")
    amazonKinesisAsyncClientBuilder.setCredentials(v)
  }
  awsClientConfig.clientConfiguration.foreach(amazonKinesisAsyncClientBuilder.setClientConfiguration)
  awsClientConfig.endpointConfiguration.foreach { v =>
    logger.info(s"serviceEndpoint = ${v.getServiceEndpoint}")
    logger.info(s"signingRegion = ${v.getSigningRegion}")
    amazonKinesisAsyncClientBuilder.setEndpointConfiguration(v)
  }
  if (awsClientConfig.endpointConfiguration.isEmpty) {
    logger.info(s"region = ${awsClientConfig.region.getName}")
    amazonKinesisAsyncClientBuilder.setRegion(awsClientConfig.region.getName)
  }
  val underlying: AmazonKinesisAsync = amazonKinesisAsyncClientBuilder.build()

  // --- wait streams

  def waitStreamsToCreated(): Try[Unit] = {
    listStreams().flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(())) {
        case (_, streamName) =>
          waitStreamToCreated(streamName)
      }
    }
  }

  // --- wait stream

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1 seconds): Try[Unit] = {
    def go: Try[Unit] = {
      describeStream(streamName) match {
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

  // ---

  def deleteStreamsAsync(implicit ec: ExecutionContext): Future[Seq[DeleteStreamResult]] = {
    listStreamsAsync.flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Future.successful(Seq.empty[DeleteStreamResult])) {
        case (resultsTry, e) =>
          for {
            results <- resultsTry
            result  <- deleteStreamAsync(e)
          } yield results :+ result
      }
    }
  }

  def deleteStreams(): Try[Seq[DeleteStreamResult]] = {
    listStreams().flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(Seq.empty[DeleteStreamResult])) {
        case (resultsTry, e) =>
          for {
            results <- resultsTry
            result  <- deleteStream(e)
          } yield results :+ result
      }
    }
  }

}
