package com.github.j5ik2o.ak.persistence.serialization

import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }
import com.github.j5ik2o.ak.persistence.util.TrySeq

import scala.util.Try

case class PersistentReprWithTags(persistentRepr: PersistentRepr, tags: Set[String])

trait PersistentReprSerializer[T] {

  def serialize(messages: Seq[AtomicWrite]): Seq[Try[Seq[T]]] = {
    messages.map { atomicWrite =>
      val serialized = atomicWrite.payload.map(serialize)
      TrySeq.sequence(serialized)
    }
  }

  def serialize(persistentRepr: PersistentRepr): Try[T] = persistentRepr.payload match {
    case Tagged(payload, tags) =>
      serialize(PersistentReprWithTags(persistentRepr.withPayload(payload), tags))
    case _ => serialize(PersistentReprWithTags(persistentRepr, Set.empty[String]))
  }

  def serialize(persistentReprWithTags: PersistentReprWithTags): Try[T]

  def deserialize(t: T): Try[PersistentReprWithTags]

}
