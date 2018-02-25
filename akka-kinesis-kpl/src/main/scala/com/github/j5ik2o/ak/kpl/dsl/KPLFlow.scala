package com.github.j5ik2o.ak.kpl.dsl

import akka.stream.scaladsl.Flow
import com.amazonaws.services.kinesis.producer.{
  KinesisProducer,
  KinesisProducerConfiguration,
  UserRecord,
  UserRecordResult
}
import com.github.j5ik2o.ak.kpl.stage.KPLFlowStage

import scala.concurrent.{ ExecutionContext, Future }

object KPLFlow {

  def apply(streamName: String, kinesisProducerConfiguration: KinesisProducerConfiguration, settings: KPLFlowSettings)(
      implicit ec: ExecutionContext
  ): Flow[UserRecord, UserRecordResult, Future[KinesisProducer]] = {
    Flow.fromGraph(
      new KPLFlowStage(
        kinesisProducerConfiguration,
        settings.maxRetries,
        settings.backoffStrategy,
        settings.retryInitialTimeout
      )
    )
  }

}
