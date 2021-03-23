package com.github.j5ik2o.ak.kcl.dsl

import akka.stream.scaladsl.Source
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

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService, Future }

object KCLSource {

  def apply(
      kinesisClientLibConfiguration: KinesisClientLibConfiguration,
      checkWorkerPeriodicity: FiniteDuration = 1.seconds,
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None,
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
      shardSyncerOpt: Option[ShardSyncer] = None
  )(implicit ec: ExecutionContext): Source[Record, Future[Worker]] = {
    ofCustomWorker(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        recordProcessorFactoryOpt,
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
        shardSyncerOpt
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
      recordProcessorFactoryOpt: Option[IRecordProcessorFactory] = None,
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
      shardSyncerOpt: Option[ShardSyncer] = None
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] = {
    ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity,
      KCLSourceStage.newDefaultWorker(
        kinesisClientLibConfiguration,
        recordProcessorFactoryOpt,
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
        shardSyncerOpt
      )
    )
  }

  def ofCustomWorkerWithoutCheckpoint(
      checkWorkerPeriodicity: FiniteDuration = 1 seconds,
      workerF: WorkerF
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Worker]] =
    Source.fromGraph(new KCLSourceStage(checkWorkerPeriodicity, workerF))

}
