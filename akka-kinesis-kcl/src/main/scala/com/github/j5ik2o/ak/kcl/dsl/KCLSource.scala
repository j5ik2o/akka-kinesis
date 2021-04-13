package com.github.j5ik2o.ak.kcl.dsl

import akka.stream.scaladsl.Source
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  LeaderDecider,
  ShardPrioritization,
  ShardSyncer,
  Worker,
  WorkerStateChangeListener
}
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease
import com.amazonaws.services.kinesis.leases.interfaces.{ ILeaseManager, ILeaseRenewer, ILeaseTaker, LeaseSelector }
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.KCLSourceStage.WorkerF
import com.github.j5ik2o.ak.kcl.stage.{ CommittableRecord, KCLSourceStage }
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration.ConfigOverrides
import com.typesafe.config.Config

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future }

object KCLSource {

  def apply(
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
      configOverrides: Option[ConfigOverrides] = None,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      amazonKinesisOpt: Option[AmazonKinesis] = None,
      amazonDynamoDBOpt: Option[AmazonDynamoDB] = None,
      amazonCloudWatchOpt: Option[AmazonCloudWatch] = None,
      iMetricsFactoryOpt: Option[IMetricsFactory] = None,
      leaseManagerOpt: Option[ILeaseManager[KinesisClientLease]],
      executionContextExecutorServiceOpt: Option[ExecutionContextExecutorService] = None,
      shardPrioritizationOpt: Option[ShardPrioritization] = None,
      kinesisProxyOpt: Option[IKinesisProxy] = None,
      workerStateChangeListenerOpt: Option[WorkerStateChangeListener] = None,
      leaseSelectorOpt: Option[LeaseSelector[KinesisClientLease]] = None,
      leaderDeciderOpt: Option[LeaderDecider] = None,
      leaseTakerOpt: Option[ILeaseTaker[KinesisClientLease]] = None,
      leaseRenewerOpt: Option[ILeaseRenewer[KinesisClientLease]] = None,
      shardSyncerOpt: Option[ShardSyncer] = None,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] = {
    val kinesisClientLibConfiguration = KCLConfiguration.fromConfig(
      config,
      applicationName,
      workerId,
      streamArn,
      kinesisCredentialsProvider,
      dynamoDBCredentialsProvider,
      cloudWatchCredentialsProvider,
      kinesisEndpoint,
      dynamoDBEndpoint,
      region,
      kinesisClientConfig,
      dynamoDBClientConfig,
      dynamoDBClientConfig
    )
    ofCustomWorker(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        amazonKinesisOpt,
        amazonDynamoDBOpt,
        amazonCloudWatchOpt,
        iMetricsFactoryOpt,
        leaseManagerOpt,
        executionContextExecutorServiceOpt,
        shardPrioritizationOpt,
        kinesisProxyOpt,
        workerStateChangeListenerOpt,
        leaseSelectorOpt,
        leaderDeciderOpt,
        leaseTakerOpt,
        leaseRenewerOpt,
        shardSyncerOpt,
        recordProcessorFactoryOpt
      )
    )
  }

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      amazonKinesisOpt: Option[AmazonKinesis] = None,
      amazonDynamoDBOpt: Option[AmazonDynamoDB] = None,
      amazonCloudWatchOpt: Option[AmazonCloudWatch] = None,
      iMetricsFactoryOpt: Option[IMetricsFactory] = None,
      leaseManagerOpt: Option[ILeaseManager[KinesisClientLease]],
      executionContextExecutorServiceOpt: Option[ExecutionContextExecutorService] = None,
      shardPrioritizationOpt: Option[ShardPrioritization] = None,
      kinesisProxyOpt: Option[IKinesisProxy] = None,
      workerStateChangeListenerOpt: Option[WorkerStateChangeListener] = None,
      leaseSelectorOpt: Option[LeaseSelector[KinesisClientLease]] = None,
      leaderDeciderOpt: Option[LeaderDecider] = None,
      leaseTakerOpt: Option[ILeaseTaker[KinesisClientLease]] = None,
      leaseRenewerOpt: Option[ILeaseRenewer[KinesisClientLease]] = None,
      shardSyncerOpt: Option[ShardSyncer] = None,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] = {
    ofCustomWorker(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        amazonKinesisOpt,
        amazonDynamoDBOpt,
        amazonCloudWatchOpt,
        iMetricsFactoryOpt,
        leaseManagerOpt,
        executionContextExecutorServiceOpt,
        shardPrioritizationOpt,
        kinesisProxyOpt,
        workerStateChangeListenerOpt,
        leaseSelectorOpt,
        leaderDeciderOpt,
        leaseTakerOpt,
        leaseRenewerOpt,
        shardSyncerOpt,
        recordProcessorFactoryOpt
      )
    )
  }

  def ofCustomWorker(
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      workerF: WorkerF
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] =
    ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity,
      workerF
    ).via(KCLFlow.ofCheckpoint())

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      amazonKinesisOpt: Option[AmazonKinesis] = None,
      amazonDynamoDBOpt: Option[AmazonDynamoDB] = None,
      amazonCloudWatchOpt: Option[AmazonCloudWatch] = None,
      iMetricsFactoryOpt: Option[IMetricsFactory] = None,
      leaseManagerOpt: Option[ILeaseManager[KinesisClientLease]] = None,
      executionContextExecutorServiceOpt: Option[ExecutionContextExecutorService] = None,
      shardPrioritizationOpt: Option[ShardPrioritization] = None,
      kinesisProxyOpt: Option[IKinesisProxy] = None,
      workerStateChangeListenerOpt: Option[WorkerStateChangeListener] = None,
      leaseSelectorOpt: Option[LeaseSelector[KinesisClientLease]] = None,
      leaderDeciderOpt: Option[LeaderDecider] = None,
      leaseTakerOpt: Option[ILeaseTaker[KinesisClientLease]] = None,
      leaseRenewerOpt: Option[ILeaseRenewer[KinesisClientLease]] = None,
      shardSyncerOpt: Option[ShardSyncer] = None,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] = {}

  def withoutCheckpoint(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      amazonKinesisOpt: Option[AmazonKinesis] = None,
      amazonDynamoDBOpt: Option[AmazonDynamoDB] = None,
      amazonCloudWatchOpt: Option[AmazonCloudWatch] = None,
      iMetricsFactoryOpt: Option[IMetricsFactory] = None,
      leaseManagerOpt: Option[ILeaseManager[KinesisClientLease]] = None,
      executionContextExecutorServiceOpt: Option[ExecutionContextExecutorService] = None,
      shardPrioritizationOpt: Option[ShardPrioritization] = None,
      kinesisProxyOpt: Option[IKinesisProxy] = None,
      workerStateChangeListenerOpt: Option[WorkerStateChangeListener] = None,
      leaseSelectorOpt: Option[LeaseSelector[KinesisClientLease]] = None,
      leaderDeciderOpt: Option[LeaderDecider] = None,
      leaseTakerOpt: Option[ILeaseTaker[KinesisClientLease]] = None,
      leaseRenewerOpt: Option[ILeaseRenewer[KinesisClientLease]] = None,
      shardSyncerOpt: Option[ShardSyncer] = None,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] = {
    ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        amazonKinesisOpt,
        amazonDynamoDBOpt,
        amazonCloudWatchOpt,
        iMetricsFactoryOpt,
        leaseManagerOpt,
        executionContextExecutorServiceOpt,
        shardPrioritizationOpt,
        kinesisProxyOpt,
        workerStateChangeListenerOpt,
        leaseSelectorOpt,
        leaderDeciderOpt,
        leaseTakerOpt,
        leaseRenewerOpt,
        shardSyncerOpt,
        recordProcessorFactoryOpt
      )
    )
  }

  def ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      workerF: WorkerF
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] =
    Source.fromGraph(new KCLSourceStage(checkWorkerPeriodicity, workerF))

}
