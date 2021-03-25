package com.github.j5ik2o.ak.kcl.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  DataFetchingStrategy,
  InitialPositionInStream,
  KinesisClientLibConfiguration,
  NoOpShardPrioritization,
  ShardSyncStrategyType,
  SimpleRecordsFetcherFactory
}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import java.time.Instant
import java.util.{ Date, UUID }
import scala.concurrent.duration._

object KCLConfiguration {

  case class ConfigOverrides(
      positionInStreamOpt: Option[InitialPositionInStream] = None,
      timestampAtInitialPositionInStreamOpt: Option[Instant] = None,
      regionName: Option[String] = None,
      maxRecordsOpt: Option[Int] = None,
      idleTimeBetweenReadsOpt: Option[FiniteDuration] = None,
      failoverTimeOpt: Option[FiniteDuration] = None,
      shardSyncIntervalOpt: Option[Duration] = None,
      callProcessRecordsEvenForEmptyRecordListOpt: Option[Boolean] = None,
      parentShardPollIntervalOpt: Option[FiniteDuration] = None,
      cleanupLeasesUponShardCompletionOpt: Option[Boolean] = None,
      userAgentOpt: Option[String] = None,
      taskBackoffTimeOpt: Option[FiniteDuration] = None,
      metricsBufferTimeOpt: Option[FiniteDuration] = None,
      metricsMaxQueueSizeOpt: Option[Int] = None,
      metricsLevelOpt: Option[MetricsLevel] = None,
      billingModeOpt: Option[BillingMode] = None,
      validateSequenceNumberBeforeCheckpointingOpt: Option[Boolean] = None,
      skipShardSyncAtStartupIfLeasesExistOpt: Option[Boolean] = None,
      shardSyncStrategyTypeOpt: Option[ShardSyncStrategyType] = None,
      maxLeasesForWorkerOpt: Option[Int] = None,
      maxLeasesToStealAtOneTimeOpt: Option[Int] = None,
      initialLeaseTableReadCapacityOpt: Option[Int] = None,
      initialLeaseTableWriteCapacityOpt: Option[Int] = None,
      maxLeaseRenewalThreadsOpt: Option[Int] = None,
      maxPendingProcessRecordsInputOpt: Option[Int] = None,
      retryGetRecordsInSecondsOpt: Option[Duration] = None,
      maxGetRecordsThreadPoolOpt: Option[Int] = None,
      maxCacheByteSizeOpt: Option[Int] = None,
      dataFetchingStrategyOpt: Option[DataFetchingStrategy] = None,
      maxRecordsCountOpt: Option[Int] = None,
      timeoutOpt: Option[Duration] = None,
      shutdownGraceOpt: Option[FiniteDuration] = None,
      idleMillisBetweenCallsOpt: Option[Long] = None,
      logWarningForTaskAfterMillisOpt: Option[Duration] = None,
      listShardsBackoffTimeInMillisOpt: Option[FiniteDuration] = None,
      maxListShardsRetryAttemptsOpt: Option[Int] = None
  )

  def fromConfig(
      config: Config,
      applicationName: String,
      workerId: UUID,
      streamArn: String,
      kinesisCredentialsProvider: AWSCredentialsProvider,
      dynamoDBCredentialsProvider: AWSCredentialsProvider,
      cloudWatchCredentialsProvider: AWSCredentialsProvider,
      kinesisEndpoint: Option[String] = None,
      dynamoDBEndpoint: Option[String] = None,
      region: Option[Region] = None,
      kinesisClientConfig: ClientConfiguration = new ClientConfiguration(),
      dynamoDBClientConfig: ClientConfiguration = new ClientConfiguration(),
      cloudWatchClientConfig: ClientConfiguration = new ClientConfiguration(),
      configOverrides: Option[ConfigOverrides] = None
  ): KinesisClientLibConfiguration = {
    val position = {
      val v = InitialPositionInStream.valueOf(
        config.getOrElse[String](
          "initial-position-in-stream",
          KinesisClientLibConfiguration.DEFAULT_INITIAL_POSITION_IN_STREAM.toString
        )
      )
      configOverrides.fold(v)(_.positionInStreamOpt.getOrElse(v))
    }
    val maxRecords = {
      val v = config.getOrElse[Int]("max-records", KinesisClientLibConfiguration.DEFAULT_MAX_RECORDS)
      configOverrides.fold(v)(_.maxRecordsOpt.getOrElse(v))
    }
    val idleTimeBetweenReads = {
      val v = config.getOrElse(
        "idle-time-between-reads",
        KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS millis
      )
      configOverrides.fold(v)(_.idleTimeBetweenReadsOpt.getOrElse(v))
    }
    val failoverTime = {
      val v = config.getOrElse("failover-time", KinesisClientLibConfiguration.DEFAULT_FAILOVER_TIME_MILLIS millis)
      configOverrides.fold(v)(_.failoverTimeOpt.getOrElse(v))
    }
    val shardSyncInterval = {
      val v = config
        .getOrElse[Duration](
          "shard-sync-interval",
          KinesisClientLibConfiguration.DEFAULT_SHARD_SYNC_INTERVAL_MILLIS millis
        )
      configOverrides.fold(v)(_.shardSyncIntervalOpt.getOrElse(v))
    }
    val callProcessRecordsEvenForEmptyRecordList = {
      val v = config.getOrElse(
        "call-process-records-even-for-empty-record-list",
        KinesisClientLibConfiguration.DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST
      )
      configOverrides.fold(v)(_.callProcessRecordsEvenForEmptyRecordListOpt.getOrElse(v))
    }
    val parentShardPollInterval = {
      val v = config.getOrElse(
        "parent-shard-poll-interval",
        KinesisClientLibConfiguration.DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS millis
      )
      configOverrides.fold(v)(_.parentShardPollIntervalOpt.getOrElse(v))
    }
    val cleanupLeasesUponShardCompletion = {
      val v = config.getOrElse(
        "cleanup-leases-upon-shard-completion",
        KinesisClientLibConfiguration.DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION
      )
      configOverrides.fold(v)(_.cleanupLeasesUponShardCompletionOpt.getOrElse(v))
    }
    // val withIgnoreUnexpectedChildShards = config.getOrElse("withIgnoreUnexpectedChildShards",KinesisClientLibConfiguration. )
    val userAgent = {
      val v = config.getOrElse("user-agent", KinesisClientLibConfiguration.KINESIS_CLIENT_LIB_USER_AGENT)
      configOverrides.fold(v)(_.userAgentOpt.getOrElse(v))
    }
    val taskBackoffTime = {
      val v = config.getOrElse(
        "task-backoff-time",
        KinesisClientLibConfiguration.DEFAULT_TASK_BACKOFF_TIME_MILLIS millis
      )
      configOverrides.fold(v)(_.taskBackoffTimeOpt.getOrElse(v))
    }
    val metricsBufferTime = {
      val v = config.getOrElse(
        "metrics-buffer-time",
        KinesisClientLibConfiguration.DEFAULT_METRICS_BUFFER_TIME_MILLIS millis
      )
      configOverrides.fold(v)(_.metricsBufferTimeOpt.getOrElse(v))
    }
    val metricsMaxQueueSize = {
      val v = config.getOrElse("metrics-max-queue-size", KinesisClientLibConfiguration.DEFAULT_METRICS_MAX_QUEUE_SIZE)
      configOverrides.fold(v)(_.metricsMaxQueueSizeOpt.getOrElse(v))
    }
    val metricsLevel = {
      val v = MetricsLevel.valueOf(
        config.getOrElse("metrics-level", KinesisClientLibConfiguration.DEFAULT_METRICS_LEVEL.getName)
      )
      configOverrides.fold(v)(_.metricsLevelOpt.getOrElse(v))
    }
    val billingMode = {
      val v = BillingMode.valueOf(
        config.getOrElse("billing-mode", KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE.toString)
      )
      configOverrides.fold(v)(_.billingModeOpt.getOrElse(v))
    }
    val validateSequenceNumberBeforeCheckpointing = {
      val v = config.getOrElse(
        "validate-sequence-number-before-checkpointing",
        KinesisClientLibConfiguration.DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING
      )
      configOverrides.fold(v)(_.validateSequenceNumberBeforeCheckpointingOpt.getOrElse(v))
    }
    val skipShardSyncAtStartupIfLeasesExist = {
      val v = config.getOrElse(
        "skip-shard-sync-at-startup-if-leases-exist",
        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST
      )
      configOverrides.fold(v)(_.skipShardSyncAtStartupIfLeasesExistOpt.getOrElse(v))
    }
    val shardSyncStrategyType = {
      val v = ShardSyncStrategyType.valueOf(
        config.getOrElse(
          "shard-sync-strategy-type",
          KinesisClientLibConfiguration.DEFAULT_SHARD_SYNC_STRATEGY_TYPE.toString
        )
      )
      configOverrides.fold(v)(_.shardSyncStrategyTypeOpt.getOrElse(v))
    }
    val maxLeasesForWorker = {
      val v = config.getOrElse("max-leases-for-worker", KinesisClientLibConfiguration.DEFAULT_MAX_LEASES_FOR_WORKER)
      configOverrides.fold(v)(_.maxLeasesForWorkerOpt.getOrElse(v))
    }
    val maxLeasesToStealAtOneTime = {
      val v = config.getOrElse(
        "max-leases-to-steal-at-one-time",
        KinesisClientLibConfiguration.DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME
      )
      configOverrides.fold(v)(_.maxLeasesToStealAtOneTimeOpt.getOrElse(v))
    }
    val initialLeaseTableReadCapacity = {
      val v = config.getOrElse(
        "initial-lease-table-read-capacity",
        KinesisClientLibConfiguration.DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY
      )
      configOverrides.fold(v)(_.initialLeaseTableReadCapacityOpt.getOrElse(v))
    }
    val initialLeaseTableWriteCapacity = {
      val v = config.getOrElse(
        "initial-lease-table-write-capacity",
        KinesisClientLibConfiguration.DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY
      )
      configOverrides.fold(v)(_.initialLeaseTableWriteCapacityOpt.getOrElse(v))
    }
    val maxLeaseRenewalThreads = {
      val v =
        config.getOrElse("max-lease-renewal-threads", KinesisClientLibConfiguration.DEFAULT_MAX_LEASE_RENEWAL_THREADS)
      configOverrides.fold(v)(_.maxLeaseRenewalThreadsOpt.getOrElse(v))
    }

    val maxPendingProcessRecordsInput = {
      val v = config.getAs[Int]("max-pending-process-records-input")
      configOverrides.fold(v)(_.maxPendingProcessRecordsInputOpt.orElse(v))
    }
    val retryGetRecordsInSeconds = {
      val v = config.getAs[Duration]("retry-get-records")
      configOverrides.fold(v)(_.retryGetRecordsInSecondsOpt.orElse(v))
    }
    val maxGetRecordsThreadPool = {
      val v = config.getAs[Int]("max-get-records-thread-pool")
      configOverrides.fold(v)(_.maxGetRecordsThreadPoolOpt.orElse(v))
    }
    val maxCacheByteSize = {
      val v = config.getAs[Int]("max-cache-byte-size")
      configOverrides.fold(v)(_.maxCacheByteSizeOpt.orElse(v))
    }
    val dataFetchingStrategy = {
      val v = config.getAs[String]("data-fetching-strategy")
      configOverrides.fold(v)(_.dataFetchingStrategyOpt.map(_.toString).orElse(v))
    }
    val maxRecordsCount = {
      val v = config.getAs[Int]("max-records-count")
      configOverrides.fold(v)(_.maxRecordsCountOpt.orElse(v))
    }
    val timeout = {
      val v = config.getAs[Duration]("timeout")
      configOverrides.fold(v)(_.timeoutOpt.orElse(v))
    }
    val shutdownGrace = {
      val v = config.getOrElse("shutdown-grace", KinesisClientLibConfiguration.DEFAULT_SHUTDOWN_GRACE_MILLIS millis)
      configOverrides.fold(v)(_.shutdownGraceOpt.getOrElse(v))
    }
    val idleMillisBetweenCalls = {
      val v = config.getAs[Long]("idle-millis-between-calls")
      configOverrides.fold(v)(_.idleMillisBetweenCallsOpt.orElse(v))
    }
    val logWarningForTaskAfterMillis = {
      val v = config.getAs[Duration]("log-warning-for-task-after")
      configOverrides.fold(v)(_.logWarningForTaskAfterMillisOpt.orElse(v))
    }
    val listShardsBackoffTimeInMillis = {
      val v = config.getOrElse(
        "list-shards-backoff-time",
        KinesisClientLibConfiguration.DEFAULT_LIST_SHARDS_BACKOFF_TIME_IN_MILLIS millis
      )
      configOverrides.fold(v)(_.listShardsBackoffTimeInMillisOpt.getOrElse(v))
    }
    val maxListShardsRetryAttempts = {
      val v = config.getOrElse(
        "max-list-shards-retry-attempts",
        KinesisClientLibConfiguration.DEFAULT_MAX_LIST_SHARDS_RETRY_ATTEMPTS
      )
      configOverrides.fold(v)(_.maxListShardsRetryAttemptsOpt.getOrElse(v))
    }

    val baseWorkerConfig = new KinesisClientLibConfiguration(
      applicationName,
      streamArn,
      kinesisEndpoint.orNull,
      dynamoDBEndpoint.orNull,
      position,
      kinesisCredentialsProvider,
      dynamoDBCredentialsProvider,
      cloudWatchCredentialsProvider,
      failoverTime.toMillis,
      workerId.toString,
      maxRecords,
      idleTimeBetweenReads.toMillis,
      callProcessRecordsEvenForEmptyRecordList,
      parentShardPollInterval.toMillis,
      shardSyncInterval.toMillis,
      cleanupLeasesUponShardCompletion,
      kinesisClientConfig,
      dynamoDBClientConfig,
      cloudWatchClientConfig,
      taskBackoffTime.toMillis,
      metricsBufferTime.toMillis,
      metricsMaxQueueSize,
      validateSequenceNumberBeforeCheckpointing,
      region.map(_.toString).orNull,
      shutdownGrace.toMillis,
      billingMode,
      new SimpleRecordsFetcherFactory(),
      java.time.Duration.ofMinutes(1).toMillis,
      java.time.Duration.ofMinutes(5).toMillis,
      java.time.Duration.ofMinutes(30).toMillis
    ).withCallProcessRecordsEvenForEmptyRecordList(callProcessRecordsEvenForEmptyRecordList)
      .withParentShardPollIntervalMillis(parentShardPollInterval.toMillis)
      // withIgnoreUnexpectedChildShards
      // withCommonClientConfig
      // withKinesisClientConfig
      // withDynamoDBClientConfig
      // withCloudWatchClientConfig
      .withUserAgent(userAgent)
      .withMetricsMaxQueueSize(metricsMaxQueueSize)
      .withMetricsLevel(metricsLevel)
      // withMetricsLevel
      // withMetricsEnabledDimensions
      .withSkipShardSyncAtStartupIfLeasesExist(skipShardSyncAtStartupIfLeasesExist)
      .withShardSyncStrategyType(shardSyncStrategyType)
      .withMaxLeasesForWorker(maxLeasesForWorker)
      .withMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime)
      .withInitialLeaseTableReadCapacity(initialLeaseTableReadCapacity)
      .withInitialLeaseTableWriteCapacity(initialLeaseTableWriteCapacity)
      .withShardPrioritizationStrategy(new NoOpShardPrioritization())
      .withMaxLeaseRenewalThreads(maxLeaseRenewalThreads)
      .withListShardsBackoffTimeInMillis(listShardsBackoffTimeInMillis.toMillis)
      .withMaxListShardsRetryAttempts(maxListShardsRetryAttempts)

    val c1 = configOverrides.fold(baseWorkerConfig)(_.timestampAtInitialPositionInStreamOpt.fold(baseWorkerConfig) {
      instant => baseWorkerConfig.withTimestampAtInitialPositionInStream(Date.from(instant))
    })
    val c2  = configOverrides.fold(c1)(_.regionName.fold(c1) { v => c1.withRegionName(v) })
    val c3  = retryGetRecordsInSeconds.fold(c2) { v => c2.withRetryGetRecordsInSeconds(v.toSeconds.toInt) }
    val c4  = maxGetRecordsThreadPool.fold(c3) { v => c3.withMaxGetRecordsThreadPool(v) }
    val c5  = maxPendingProcessRecordsInput.fold(c4) { v => c4.withMaxPendingProcessRecordsInput(v) }
    val c6  = maxCacheByteSize.fold(c5) { v => c5.withMaxCacheByteSize(v) }
    val c7  = dataFetchingStrategy.fold(c6) { v => c6.withDataFetchingStrategy(v) }
    val c8  = maxRecordsCount.fold(c7) { v => c7.withMaxRecordsCount(v) }
    val c9  = timeout.fold(c8) { v => c8.withTimeoutInSeconds(v.toSeconds.toInt); c8 }
    val c10 = idleMillisBetweenCalls.fold(c9) { v => c9.withIdleMillisBetweenCalls(v) }
    logWarningForTaskAfterMillis.fold(c10) { v => c10.withLogWarningForTaskAfterMillis(v.toMillis) }
  }
}
