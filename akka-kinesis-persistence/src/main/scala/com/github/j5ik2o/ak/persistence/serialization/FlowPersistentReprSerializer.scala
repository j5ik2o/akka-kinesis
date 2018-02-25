package com.github.j5ik2o.ak.persistence.serialization

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Flow

import scala.util.Try

trait FlowPersistentReprSerializer[T] extends PersistentReprSerializer[T] {

  def deserializeFlow: Flow[T, Try[PersistentReprWithTags], NotUsed] = {
    Flow[T].map(deserialize)
  }

  def deserializeFlowWithoutTags: Flow[T, Try[PersistentRepr], NotUsed] = {
    def keepPersistentRepr(tup: PersistentReprWithTags): PersistentRepr = tup match {
      case PersistentReprWithTags(repr, _) => repr
    }
    deserializeFlow.map(_.map(keepPersistentRepr))
  }

}
