package com.github.j5ik2o.ak.persistence.model

import com.github.j5ik2o.ak.persistence.serialization.PersistentReprSerializer

case class JournalRowId(persistenceId: String, sequenceNr: Long)

case class JournalRow(id: JournalRowId, deleted: Boolean, message: Array[Byte], tags: Option[String]) {

  def messageAsString(implicit serializer: PersistentReprSerializer[JournalRow]): String = {
    serializer
      .deserialize(this)
      .map(_.persistentRepr)
      .map { pr =>
        s"PersistentRepr(persistenceId = ${pr.persistenceId}, sequenceNr = ${pr.sequenceNr}," +
        s" payload = ${pr.payload}, deleted = ${pr.deleted}, manifest = ${pr.manifest})"
      }
      .getOrElse("Can't deserialize value")
  }

  def encodeString(implicit serializer: PersistentReprSerializer[JournalRow]) = {
    s"JournalRow($id, $deleted, $message, $tags)"
  }

}
