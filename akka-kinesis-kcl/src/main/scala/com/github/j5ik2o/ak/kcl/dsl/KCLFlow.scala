package com.github.j5ik2o.ak.kcl.dsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.amazonaws.services.kinesis.model.Record
import com.github.j5ik2o.ak.kcl.stage.CommittableRecord

import scala.concurrent.{ ExecutionContext, Future }

object KCLFlow {

  def ofCheckpoint()(implicit ec: ExecutionContext): Flow[CommittableRecord, Record, NotUsed] =
    Flow[CommittableRecord]
      .mapAsync(1) { v =>
        if (v.canBeCheckpointed) {
          v.checkpoint().map { _ => v }
        } else Future.successful(v)
      }
      .map(_.record)

}
