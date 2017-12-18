package com.github.j5ik2o.ak.kpl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }

import scala.concurrent.ExecutionContext

object KPLFlow {

  def apply(
      kinesisProducerConfiguration: KinesisProducerConfiguration,
      settings: KPLFLowSettings
  )(implicit ec: ExecutionContext): Flow[UserRecord, UserRecordResult, NotUsed] =
    Flow.fromGraph(new KPLFlowStage(kinesisProducerConfiguration, settings))

}
