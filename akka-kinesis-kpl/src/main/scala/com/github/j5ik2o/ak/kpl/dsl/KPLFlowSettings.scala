package com.github.j5ik2o.ak.kpl.dsl

import scala.concurrent.duration.{ FiniteDuration, _ }

object KPLFlowSettings {

  private val MAX_RECORDS_PER_REQUEST          = 500
  private val MAX_RECORDS_PER_SHARD_PER_SECOND = 1000
  private val MAX_BYTES_PER_SHARD_PER_SECOND   = 1000000

  sealed trait RetryBackoffStrategy
  case object Exponential extends RetryBackoffStrategy
  case object Lineal      extends RetryBackoffStrategy

  val exponential = Exponential
  val lineal      = Lineal

  val defaultInstance: KPLFlowSettings = byNumberOfShards(1)

  def byNumberOfShards(shards: Int): KPLFlowSettings =
    KPLFlowSettings(
      parallelism = shards * (MAX_RECORDS_PER_SHARD_PER_SECOND / MAX_RECORDS_PER_REQUEST),
      maxBatchSize = MAX_RECORDS_PER_REQUEST,
      maxRecordsPerSecond = shards * MAX_RECORDS_PER_SHARD_PER_SECOND,
      maxBytesPerSecond = shards * MAX_BYTES_PER_SHARD_PER_SECOND,
      maxRetries = 5,
      backoffStrategy = Exponential,
      retryInitialTimeout = 100 millis
    )

}

case class KPLFlowSettings(
    parallelism: Int,
    maxBatchSize: Int,
    maxRecordsPerSecond: Int,
    maxBytesPerSecond: Int,
    maxRetries: Int = 5,
    backoffStrategy: KPLFlowSettings.RetryBackoffStrategy = KPLFlowSettings.Exponential,
    retryInitialTimeout: FiniteDuration = 100 millis
)
