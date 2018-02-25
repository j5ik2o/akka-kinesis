package com.github.j5ik2o.ak.persistence.serialization

import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.github.j5ik2o.ak.persistence.model.{ JournalRow, JournalRowId }

import scala.util.Try

class ByteArrayJournalSerializer(serialization: Serialization, tagSeparator: String = ",")
    extends FlowPersistentReprSerializer[JournalRow] {

  override def serialize(persistentReprWithTags: PersistentReprWithTags): Try[JournalRow] =
    serialization
      .serialize(persistentReprWithTags.persistentRepr)
      .map(
        JournalRow(
          JournalRowId(persistentReprWithTags.persistentRepr.persistenceId,
                       persistentReprWithTags.persistentRepr.sequenceNr),
          persistentReprWithTags.persistentRepr.deleted,
          _,
          encodeTags(persistentReprWithTags.tags, tagSeparator)
        )
      )

  override def deserialize(journalRow: JournalRow): Try[PersistentReprWithTags] =
    serialization
      .deserialize(journalRow.message, classOf[PersistentRepr])
      .map(PersistentReprWithTags(_, decodeTags(journalRow.tags, tagSeparator)))

}
