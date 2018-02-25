package com.github.j5ik2o.ak.aws

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters
import com.amazonaws.{ AmazonWebServiceRequest, ResponseMetadata }

import scala.util.Try

trait AwsKinesis {
  val underlying: AmazonKinesis

  def addTagsToStream(addTagsToStreamRequest: AddTagsToStreamRequest): Try[AddTagsToStreamResult] = Try {
    underlying.addTagsToStream(addTagsToStreamRequest)
  }

  def createStream(createStreamRequest: CreateStreamRequest): Try[CreateStreamResult] = Try {
    underlying.createStream(createStreamRequest)
  }

  def createStream(streamName: String, shardCount: Int): Try[CreateStreamResult] = Try {
    underlying.createStream(streamName, shardCount)
  }

  def decreaseStreamRetentionPeriod(
      decreaseStreamRetentionPeriodRequest: DecreaseStreamRetentionPeriodRequest
  ): Try[DecreaseStreamRetentionPeriodResult] = Try {
    underlying.decreaseStreamRetentionPeriod(decreaseStreamRetentionPeriodRequest)
  }

  def deleteStream(deleteStreamRequest: DeleteStreamRequest): Try[DeleteStreamResult] = Try {
    underlying.deleteStream(deleteStreamRequest)
  }

  def deleteStream(streamName: String): Try[DeleteStreamResult] = Try { underlying.deleteStream(streamName) }

  def describeLimits(describeLimitsRequest: DescribeLimitsRequest): Try[DescribeLimitsResult] = Try {
    underlying.describeLimits(describeLimitsRequest)
  }

  def describeStream(describeStreamRequest: DescribeStreamRequest): Try[DescribeStreamResult] = Try {
    underlying.describeStream(describeStreamRequest)
  }

  def describeStream(streamName: String): Try[DescribeStreamResult] = Try { underlying.describeStream(streamName) }

  def describeStream(streamName: String, exclusiveStartShardId: String): Try[DescribeStreamResult] = Try {
    underlying.describeStream(streamName)
  }

  def describeStream(streamName: String, limit: Int, exclusiveStartShardId: String): Try[DescribeStreamResult] =
    Try { underlying.describeStream(streamName) }

  def disableEnhancedMonitoring(
      disableEnhancedMonitoringRequest: DisableEnhancedMonitoringRequest
  ): Try[DisableEnhancedMonitoringResult] = Try {
    underlying.disableEnhancedMonitoring(disableEnhancedMonitoringRequest)
  }

  def enableEnhancedMonitoring(
      enableEnhancedMonitoringRequest: EnableEnhancedMonitoringRequest
  ): Try[EnableEnhancedMonitoringResult] = Try { underlying.enableEnhancedMonitoring(enableEnhancedMonitoringRequest) }

  def getRecords(getRecordsRequest: GetRecordsRequest): Try[GetRecordsResult] = Try {
    underlying.getRecords(getRecordsRequest)
  }

  def getShardIterator(getShardIteratorRequest: GetShardIteratorRequest): Try[GetShardIteratorResult] = Try {
    underlying.getShardIterator(getShardIteratorRequest)
  }

  def getShardIterator(streamName: String, shardId: String, shardIteratorType: String): Try[GetShardIteratorResult] =
    Try { underlying.getShardIterator(streamName, shardId, shardIteratorType) }

  def getShardIterator(streamName: String,
                       shardId: String,
                       shardIteratorType: String,
                       startingSequenceNumber: String): Try[GetShardIteratorResult] =
    Try { underlying.getShardIterator(streamName, shardId, shardIteratorType, startingSequenceNumber) }

  def increaseStreamRetentionPeriod(
      increaseStreamRetentionPeriodRequest: IncreaseStreamRetentionPeriodRequest
  ): Try[IncreaseStreamRetentionPeriodResult] = Try {
    underlying.increaseStreamRetentionPeriod(increaseStreamRetentionPeriodRequest)
  }

  def listStreams(listStreamsRequest: ListStreamsRequest): Try[ListStreamsResult] = Try {
    underlying.listStreams(listStreamsRequest)
  }

  def listStreams(): Try[ListStreamsResult] = Try {
    underlying.listStreams()
  }

  def listStreams(exclusiveStartStreamName: String): Try[ListStreamsResult] = Try {
    underlying.listStreams(exclusiveStartStreamName)
  }

  def listStreams(limit: Int, exclusiveStartStreamName: String): Try[ListStreamsResult] = Try {
    underlying.listStreams(limit, exclusiveStartStreamName)
  }

  def listTagsForStream(listTagsForStreamRequest: ListTagsForStreamRequest): Try[ListTagsForStreamResult] = Try {
    underlying.listTagsForStream(listTagsForStreamRequest)
  }

  def mergeShards(mergeShardsRequest: MergeShardsRequest): Try[MergeShardsResult] = Try {
    underlying.mergeShards(mergeShardsRequest)
  }

  def mergeShards(streamName: String, shardToMerge: String, adjacentShardToMerge: String): Try[MergeShardsResult] =
    Try { underlying.mergeShards(streamName, shardToMerge, adjacentShardToMerge) }

  def putRecord(putRecordRequest: PutRecordRequest): Try[PutRecordResult] = Try {
    underlying.putRecord(putRecordRequest)
  }

  def putRecord(streamName: String, data: ByteBuffer, partitionKey: String): Try[PutRecordResult] = Try {
    underlying.putRecord(streamName, data, partitionKey)
  }

  def putRecord(streamName: String,
                data: ByteBuffer,
                partitionKey: String,
                sequenceNumberForOrdering: String): Try[PutRecordResult] = Try {
    underlying.putRecord(streamName, data, partitionKey, sequenceNumberForOrdering)
  }

  def putRecords(putRecordsRequest: PutRecordsRequest): Try[PutRecordsResult] = Try {
    underlying.putRecords(putRecordsRequest)
  }

  def removeTagsFromStream(removeTagsFromStreamRequest: RemoveTagsFromStreamRequest): Try[RemoveTagsFromStreamResult] =
    Try { underlying.removeTagsFromStream(removeTagsFromStreamRequest) }

  def splitShard(splitShardRequest: SplitShardRequest): Try[SplitShardResult] = Try {
    underlying.splitShard(splitShardRequest)
  }

  def splitShard(streamName: String, shardToSplit: String, newStartingHashKey: String): Try[SplitShardResult] = Try {
    underlying.splitShard(streamName, shardToSplit, newStartingHashKey)
  }

  def updateShardCount(updateShardCountRequest: UpdateShardCountRequest): Try[UpdateShardCountResult] = Try {
    underlying.updateShardCount(updateShardCountRequest)
  }

  def shutdown(): Try[Unit] = Try { underlying.shutdown() }

  def getCachedResponseMetadata(request: AmazonWebServiceRequest): Try[ResponseMetadata] = Try {
    underlying.getCachedResponseMetadata(request)
  }

  def waiters: Try[AmazonKinesisWaiters] = Try { underlying.waiters() }
}
