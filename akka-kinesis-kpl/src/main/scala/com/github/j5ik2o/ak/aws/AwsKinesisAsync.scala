package com.github.j5ik2o.ak.aws

import java.nio.ByteBuffer

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model._

import scala.concurrent.{ ExecutionContext, Future }

trait AwsKinesisAsync {

  import com.github.j5ik2o.ak.JavaFutureConverter._

  val underlying: AmazonKinesisAsync

  def addTagsToStreamAsync(addTagsToStreamRequest: AddTagsToStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[AddTagsToStreamResult] = {
    underlying.addTagsToStreamAsync(addTagsToStreamRequest).toScala
  }

  def addTagsToStreamAsync(
      addTagsToStreamRequest: AddTagsToStreamRequest,
      asyncHandler: AsyncHandler[AddTagsToStreamRequest, AddTagsToStreamResult]
  )(implicit ec: ExecutionContext): Future[AddTagsToStreamResult] = {
    underlying.addTagsToStreamAsync(addTagsToStreamRequest, asyncHandler).toScala
  }

  def createStreamAsync(createStreamRequest: CreateStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[CreateStreamResult] = {
    underlying.createStreamAsync(createStreamRequest).toScala
  }

  def createStreamAsync(
      createStreamRequest: CreateStreamRequest,
      asyncHandler: AsyncHandler[CreateStreamRequest, CreateStreamResult]
  )(implicit ec: ExecutionContext): Future[CreateStreamResult] = {
    underlying.createStreamAsync(createStreamRequest, asyncHandler).toScala
  }

  def createStreamAsync(streamName: String, shardCount: Int)(
      implicit ec: ExecutionContext
  ): Future[CreateStreamResult] = {
    underlying.createStreamAsync(streamName, shardCount).toScala
  }

  def createStreamAsync(
      streamName: String,
      shardCount: Int,
      asyncHandler: AsyncHandler[CreateStreamRequest, CreateStreamResult]
  )(implicit ec: ExecutionContext): Future[CreateStreamResult] = {
    underlying.createStreamAsync(streamName, shardCount, asyncHandler).toScala
  }

  def decreaseStreamRetentionPeriodAsync(decreaseStreamRetentionPeriodRequest: DecreaseStreamRetentionPeriodRequest)(
      implicit ec: ExecutionContext
  ): Future[DecreaseStreamRetentionPeriodResult] = {
    underlying.decreaseStreamRetentionPeriodAsync(decreaseStreamRetentionPeriodRequest).toScala
  }

  def decreaseStreamRetentionPeriodAsync(
      decreaseStreamRetentionPeriodRequest: DecreaseStreamRetentionPeriodRequest,
      asyncHandler: AsyncHandler[DecreaseStreamRetentionPeriodRequest, DecreaseStreamRetentionPeriodResult]
  )(implicit ec: ExecutionContext): Future[DecreaseStreamRetentionPeriodResult] = {
    underlying.decreaseStreamRetentionPeriodAsync(decreaseStreamRetentionPeriodRequest, asyncHandler).toScala
  }

  def deleteStreamAsync(deleteStreamRequest: DeleteStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[DeleteStreamResult] = {
    underlying.deleteStreamAsync(deleteStreamRequest).toScala
  }

  def deleteStreamAsync(
      deleteStreamRequest: DeleteStreamRequest,
      asyncHandler: AsyncHandler[DeleteStreamRequest, DeleteStreamResult]
  )(implicit ec: ExecutionContext): Future[DeleteStreamResult] = {
    underlying.deleteStreamAsync(deleteStreamRequest, asyncHandler).toScala
  }

  def deleteStreamAsync(streamName: String)(implicit ec: ExecutionContext): Future[DeleteStreamResult] = {
    underlying.deleteStreamAsync(streamName).toScala
  }

  def deleteStreamAsync(streamName: String, asyncHandler: AsyncHandler[DeleteStreamRequest, DeleteStreamResult])(
      implicit ec: ExecutionContext
  ): Future[DeleteStreamResult] = {
    underlying.deleteStreamAsync(streamName, asyncHandler).toScala
  }

  def describeLimitsAsync(describeLimitsRequest: DescribeLimitsRequest)(
      implicit ec: ExecutionContext
  ): Future[DescribeLimitsResult] = {
    underlying.describeLimitsAsync(describeLimitsRequest).toScala
  }

  def describeLimitsAsync(
      describeLimitsRequest: DescribeLimitsRequest,
      asyncHandler: AsyncHandler[DescribeLimitsRequest, DescribeLimitsResult]
  )(implicit ec: ExecutionContext): Future[DescribeLimitsResult] = {
    underlying.describeLimitsAsync(describeLimitsRequest, asyncHandler).toScala
  }

  def describeStreamAsync(describeStreamRequest: DescribeStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(describeStreamRequest).toScala
  }

  def describeStreamAsync(
      describeStreamRequest: DescribeStreamRequest,
      asyncHandler: AsyncHandler[DescribeStreamRequest, DescribeStreamResult]
  )(implicit ec: ExecutionContext): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(describeStreamRequest, asyncHandler).toScala
  }

  def describeStreamAsync(streamName: String)(implicit ec: ExecutionContext): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName).toScala
  }

  def describeStreamAsync(streamName: String, asyncHandler: AsyncHandler[DescribeStreamRequest, DescribeStreamResult])(
      implicit ec: ExecutionContext
  ): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName, asyncHandler).toScala
  }

  def describeStreamAsync(streamName: String, exclusiveStartShardId: String)(
      implicit ec: ExecutionContext
  ): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName, exclusiveStartShardId).toScala
  }

  def describeStreamAsync(
      streamName: String,
      exclusiveStartShardId: String,
      asyncHandler: AsyncHandler[DescribeStreamRequest, DescribeStreamResult]
  )(implicit ec: ExecutionContext): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName, exclusiveStartShardId, asyncHandler).toScala
  }

  def describeStreamAsync(streamName: String, limit: Int, exclusiveStartShardId: String)(
      implicit ec: ExecutionContext
  ): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName, limit, exclusiveStartShardId).toScala
  }

  def describeStreamAsync(
      streamName: String,
      limit: Int,
      exclusiveStartShardId: String,
      asyncHandler: AsyncHandler[DescribeStreamRequest, DescribeStreamResult]
  )(implicit ec: ExecutionContext): Future[DescribeStreamResult] = {
    underlying.describeStreamAsync(streamName, limit, exclusiveStartShardId, asyncHandler).toScala
  }

  def disableEnhancedMonitoringAsync(disableEnhancedMonitoringRequest: DisableEnhancedMonitoringRequest)(
      implicit ec: ExecutionContext
  ): Future[DisableEnhancedMonitoringResult] = {
    underlying.disableEnhancedMonitoringAsync(disableEnhancedMonitoringRequest).toScala
  }

  def disableEnhancedMonitoringAsync(
      disableEnhancedMonitoringRequest: DisableEnhancedMonitoringRequest,
      asyncHandler: AsyncHandler[DisableEnhancedMonitoringRequest, DisableEnhancedMonitoringResult]
  )(implicit ec: ExecutionContext): Future[DisableEnhancedMonitoringResult] = {
    underlying.disableEnhancedMonitoringAsync(disableEnhancedMonitoringRequest, asyncHandler).toScala
  }

  def enableEnhancedMonitoringAsync(enableEnhancedMonitoringRequest: EnableEnhancedMonitoringRequest)(
      implicit ec: ExecutionContext
  ): Future[EnableEnhancedMonitoringResult] = {
    underlying.enableEnhancedMonitoringAsync(enableEnhancedMonitoringRequest).toScala
  }

  def enableEnhancedMonitoringAsync(
      enableEnhancedMonitoringRequest: EnableEnhancedMonitoringRequest,
      asyncHandler: AsyncHandler[EnableEnhancedMonitoringRequest, EnableEnhancedMonitoringResult]
  )(implicit ec: ExecutionContext): Future[EnableEnhancedMonitoringResult] = {
    underlying.enableEnhancedMonitoringAsync(enableEnhancedMonitoringRequest, asyncHandler).toScala
  }

  def getRecordsAsync(getRecordsRequest: GetRecordsRequest)(implicit ec: ExecutionContext): Future[GetRecordsResult] = {
    underlying.getRecordsAsync(getRecordsRequest).toScala
  }

  def getRecordsAsync(
      getRecordsRequest: GetRecordsRequest,
      asyncHandler: AsyncHandler[GetRecordsRequest, GetRecordsResult]
  )(implicit ec: ExecutionContext): Future[GetRecordsResult] = {
    underlying.getRecordsAsync(getRecordsRequest, asyncHandler).toScala
  }

  def getShardIteratorAsync(getShardIteratorRequest: GetShardIteratorRequest)(
      implicit ec: ExecutionContext
  ): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(getShardIteratorRequest).toScala
  }

  def getShardIteratorAsync(
      getShardIteratorRequest: GetShardIteratorRequest,
      asyncHandler: AsyncHandler[GetShardIteratorRequest, GetShardIteratorResult]
  )(implicit ec: ExecutionContext): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(getShardIteratorRequest).toScala
  }

  def getShardIteratorAsync(streamName: String, shardId: String, shardIteratorType: String)(
      implicit ec: ExecutionContext
  ): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(streamName, shardId, shardIteratorType).toScala
  }

  def getShardIteratorAsync(
      streamName: String,
      shardId: String,
      shardIteratorType: String,
      asyncHandler: AsyncHandler[GetShardIteratorRequest, GetShardIteratorResult]
  )(implicit ec: ExecutionContext): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(streamName, shardId, shardIteratorType).toScala
  }

  def getShardIteratorAsync(
      streamName: String,
      shardId: String,
      shardIteratorType: String,
      startingSequenceNumber: String
  )(implicit ec: ExecutionContext): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(streamName, shardId, shardIteratorType).toScala
  }

  def getShardIteratorAsync(
      streamName: String,
      shardId: String,
      shardIteratorType: String,
      startingSequenceNumber: String,
      asyncHandler: AsyncHandler[GetShardIteratorRequest, GetShardIteratorResult]
  )(implicit ec: ExecutionContext): Future[GetShardIteratorResult] = {
    underlying.getShardIteratorAsync(streamName, shardId, shardIteratorType, asyncHandler).toScala
  }

  def increaseStreamRetentionPeriodAsync(increaseStreamRetentionPeriodRequest: IncreaseStreamRetentionPeriodRequest)(
      implicit ec: ExecutionContext
  ): Future[IncreaseStreamRetentionPeriodResult] = {
    underlying.increaseStreamRetentionPeriodAsync(increaseStreamRetentionPeriodRequest).toScala
  }

  def increaseStreamRetentionPeriodAsync(
      increaseStreamRetentionPeriodRequest: IncreaseStreamRetentionPeriodRequest,
      asyncHandler: AsyncHandler[IncreaseStreamRetentionPeriodRequest, IncreaseStreamRetentionPeriodResult]
  )(implicit ec: ExecutionContext): Future[IncreaseStreamRetentionPeriodResult] = {
    underlying.increaseStreamRetentionPeriodAsync(increaseStreamRetentionPeriodRequest, asyncHandler).toScala
  }

  def listStreamsAsync(
      listStreamsRequest: ListStreamsRequest
  )(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(listStreamsRequest).toScala
  }

  def listStreamsAsync(
      listStreamsRequest: ListStreamsRequest,
      asyncHandler: AsyncHandler[ListStreamsRequest, ListStreamsResult]
  )(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(listStreamsRequest, asyncHandler).toScala
  }

  def listStreamsAsync()(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync().toScala
  }

  def listStreamsAsync(asyncHandler: AsyncHandler[ListStreamsRequest, ListStreamsResult])(
      implicit ec: ExecutionContext
  ): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(asyncHandler).toScala
  }

  def listStreamsAsync(exclusiveStartStreamName: String)(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(exclusiveStartStreamName).toScala
  }

  def listStreamsAsync(
      exclusiveStartStreamName: String,
      asyncHandler: AsyncHandler[ListStreamsRequest, ListStreamsResult]
  )(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(exclusiveStartStreamName, asyncHandler).toScala
  }

  def listStreamsAsync(limit: Int, exclusiveStartStreamName: String)(
      implicit ec: ExecutionContext
  ): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(limit, exclusiveStartStreamName).toScala
  }

  def listStreamsAsync(
      limit: Int,
      exclusiveStartStreamName: String,
      asyncHandler: AsyncHandler[ListStreamsRequest, ListStreamsResult]
  )(implicit ec: ExecutionContext): Future[ListStreamsResult] = {
    underlying.listStreamsAsync(limit, exclusiveStartStreamName, asyncHandler).toScala
  }

  def listTagsForStreamAsync(listTagsForStreamRequest: ListTagsForStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[ListTagsForStreamResult] = {
    underlying.listTagsForStreamAsync(listTagsForStreamRequest).toScala
  }

  def listTagsForStreamAsync(
      listTagsForStreamRequest: ListTagsForStreamRequest,
      asyncHandler: AsyncHandler[ListTagsForStreamRequest, ListTagsForStreamResult]
  )(implicit ec: ExecutionContext): Future[ListTagsForStreamResult] = {
    underlying.listTagsForStreamAsync(listTagsForStreamRequest, asyncHandler).toScala
  }

  def mergeShardsAsync(
      mergeShardsRequest: MergeShardsRequest
  )(implicit ec: ExecutionContext): Future[MergeShardsResult] = {
    underlying.mergeShardsAsync(mergeShardsRequest).toScala
  }

  def mergeShardsAsync(
      mergeShardsRequest: MergeShardsRequest,
      asyncHandler: AsyncHandler[MergeShardsRequest, MergeShardsResult]
  )(implicit ec: ExecutionContext): Future[MergeShardsResult] = {
    underlying.mergeShardsAsync(mergeShardsRequest, asyncHandler).toScala
  }

  def mergeShardsAsync(streamName: String, shardToMerge: String, adjacentShardToMerge: String)(
      implicit ec: ExecutionContext
  ): Future[MergeShardsResult] = {
    underlying.mergeShardsAsync(streamName, shardToMerge, adjacentShardToMerge).toScala
  }

  def mergeShardsAsync(
      streamName: String,
      shardToMerge: String,
      adjacentShardToMerge: String,
      asyncHandler: AsyncHandler[MergeShardsRequest, MergeShardsResult]
  )(implicit ec: ExecutionContext): Future[MergeShardsResult] = {
    underlying.mergeShardsAsync(streamName, shardToMerge, adjacentShardToMerge, asyncHandler).toScala
  }

  def putRecordAsync(putRecordRequest: PutRecordRequest)(implicit ec: ExecutionContext): Future[PutRecordResult] = {
    underlying.putRecordAsync(putRecordRequest).toScala
  }

  def putRecordAsync(putRecordRequest: PutRecordRequest, asyncHandler: AsyncHandler[PutRecordRequest, PutRecordResult])(
      implicit ec: ExecutionContext
  ): Future[PutRecordResult] = {
    underlying.putRecordAsync(putRecordRequest, asyncHandler).toScala
  }

  def putRecordAsync(streamName: String, data: ByteBuffer, partitionKey: String)(
      implicit ec: ExecutionContext
  ): Future[PutRecordResult] = {
    underlying.putRecordAsync(streamName, data, partitionKey).toScala
  }

  def putRecordAsync(
      streamName: String,
      data: ByteBuffer,
      partitionKey: String,
      asyncHandler: AsyncHandler[PutRecordRequest, PutRecordResult]
  )(implicit ec: ExecutionContext): Future[PutRecordResult] = {
    underlying.putRecordAsync(streamName, data, partitionKey, asyncHandler).toScala
  }

  def putRecordAsync(streamName: String, data: ByteBuffer, partitionKey: String, sequenceNumberForOrdering: String)(
      implicit ec: ExecutionContext
  ): Future[PutRecordResult] = {
    underlying.putRecordAsync(streamName, data, partitionKey, sequenceNumberForOrdering).toScala
  }

  def putRecordAsync(
      streamName: String,
      data: ByteBuffer,
      partitionKey: String,
      sequenceNumberForOrdering: String,
      asyncHandler: AsyncHandler[PutRecordRequest, PutRecordResult]
  )(implicit ec: ExecutionContext): Future[PutRecordResult] = {
    underlying.putRecordAsync(streamName, data, partitionKey, sequenceNumberForOrdering, asyncHandler).toScala
  }

  def putRecordsAsync(putRecordsRequest: PutRecordsRequest)(implicit ec: ExecutionContext): Future[PutRecordsResult] = {
    underlying.putRecordsAsync(putRecordsRequest).toScala
  }

  def putRecordsAsync(
      putRecordsRequest: PutRecordsRequest,
      asyncHandler: AsyncHandler[PutRecordsRequest, PutRecordsResult]
  )(implicit ec: ExecutionContext): Future[PutRecordsResult] = {
    underlying.putRecordsAsync(putRecordsRequest, asyncHandler).toScala
  }

  def removeTagsFromStreamAsync(removeTagsFromStreamRequest: RemoveTagsFromStreamRequest)(
      implicit ec: ExecutionContext
  ): Future[RemoveTagsFromStreamResult] = {
    underlying.removeTagsFromStreamAsync(removeTagsFromStreamRequest).toScala
  }

  def removeTagsFromStreamAsync(
      removeTagsFromStreamRequest: RemoveTagsFromStreamRequest,
      asyncHandler: AsyncHandler[RemoveTagsFromStreamRequest, RemoveTagsFromStreamResult]
  )(implicit ec: ExecutionContext): Future[RemoveTagsFromStreamResult] = {
    underlying.removeTagsFromStreamAsync(removeTagsFromStreamRequest, asyncHandler).toScala
  }

  def splitShardAsync(splitShardRequest: SplitShardRequest)(implicit ec: ExecutionContext): Future[SplitShardResult] = {
    underlying.splitShardAsync(splitShardRequest).toScala
  }

  def splitShardAsync(
      splitShardRequest: SplitShardRequest,
      asyncHandler: AsyncHandler[SplitShardRequest, SplitShardResult]
  )(implicit ec: ExecutionContext): Future[SplitShardResult] = {
    underlying.splitShardAsync(splitShardRequest, asyncHandler).toScala
  }

  def splitShardAsync(streamName: String, shardToSplit: String, newStartingHashKey: String)(
      implicit ec: ExecutionContext
  ): Future[SplitShardResult] = {
    underlying.splitShardAsync(streamName, shardToSplit, newStartingHashKey).toScala
  }

  def splitShardAsync(
      streamName: String,
      shardToSplit: String,
      newStartingHashKey: String,
      asyncHandler: AsyncHandler[SplitShardRequest, SplitShardResult]
  )(implicit ec: ExecutionContext): Future[SplitShardResult] = {
    underlying.splitShardAsync(streamName, shardToSplit, newStartingHashKey, asyncHandler).toScala
  }

  def updateShardCountAsync(updateShardCountRequest: UpdateShardCountRequest)(
      implicit ec: ExecutionContext
  ): Future[UpdateShardCountResult] = {
    underlying.updateShardCountAsync(updateShardCountRequest).toScala
  }

  def updateShardCountAsync(
      updateShardCountRequest: UpdateShardCountRequest,
      asyncHandler: AsyncHandler[UpdateShardCountRequest, UpdateShardCountResult]
  )(implicit ec: ExecutionContext): Future[UpdateShardCountResult] = {
    underlying.updateShardCountAsync(updateShardCountRequest, asyncHandler).toScala
  }
}
