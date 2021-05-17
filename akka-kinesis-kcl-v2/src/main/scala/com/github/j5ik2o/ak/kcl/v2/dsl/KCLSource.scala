package com.github.j5ik2o.ak.kcl.v2.dsl

import akka.stream.scaladsl.Source
import com.github.j5ik2o.ak.kcl.v2.stage.KCLSourceStage.SchedulerF
import com.github.j5ik2o.ak.kcl.v2.stage.{ CommittableRecord, KCLSourceStage }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

object KCLSource {

  def ofCustomScheduler(
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds,
      schedulerF: SchedulerF
  )(implicit ec: ExecutionContext): Source[KinesisClientRecord, Future[Scheduler]] =
    ofCustomSchedulerWithoutCheckpoint(
      checkSchedulerPeriodicity,
      schedulerF
    ).via(KCLFlow.ofCheckpoint())

  def ofCustomSchedulerWithoutCheckpoint(
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds,
      schedulerF: SchedulerF
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Scheduler]] =
    Source.fromGraph(new KCLSourceStage(checkSchedulerPeriodicity, schedulerF))

}
