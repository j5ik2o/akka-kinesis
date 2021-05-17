package com.github.j5ik2o.ak.kcl.v2.dsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.ak.kcl.v2.stage.CommittableRecord
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.concurrent.ExecutionContext

object KCLFlow {

  def ofCheckpoint()(implicit ec: ExecutionContext): Flow[CommittableRecord, KinesisClientRecord, NotUsed] =
    Flow[CommittableRecord]
      .mapAsync(1) { v => v.checkpoint().map { _ => v } }
      .map(_.record)
}
