package com.github.j5ik2o.ak.persistence

import com.github.j5ik2o.ak.persistence.lifecycle.{ JournalRowRepository, JournalRowRepositoryOnMemory }
import com.github.j5ik2o.ak.persistence.model.JournalRow
import com.github.j5ik2o.ak.persistence.serialization.{ ByteArrayJournalSerializer, FlowPersistentReprSerializer }

class MockAsyncWriteJournal extends AbstractAsyncWriteJournal {

  override protected val serializer: FlowPersistentReprSerializer[JournalRow] = new ByteArrayJournalSerializer(
    serialization
  )

  override protected val repository: JournalRowRepository = new JournalRowRepositoryOnMemory()

}
