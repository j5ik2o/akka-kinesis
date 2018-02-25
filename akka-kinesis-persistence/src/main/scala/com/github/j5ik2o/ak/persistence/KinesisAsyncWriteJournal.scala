package com.github.j5ik2o.ak.persistence
import com.amazonaws.regions.Regions
import com.github.j5ik2o.ak.persistence.lifecycle.{ JournalRowRepository, JournalRowRepositoryOnKinesis }
import com.github.j5ik2o.ak.persistence.model.JournalRow
import com.github.j5ik2o.ak.persistence.serialization.{ ByteArrayJournalSerializer, FlowPersistentReprSerializer }
import com.typesafe.config.Config

class KinesisAsyncWriteJournal extends AbstractAsyncWriteJournal {

  private val config: Config     = context.system.settings.config.getConfig("kinesis-journal")
  private val streamName: String = config.getString("stream-name")
  private val regions: Regions   = Regions.fromName(config.getString("region"))
  private val numOfShards: Int   = config.getInt("num-of-shards")

  private implicit val system = context.system

  override protected val serializer: FlowPersistentReprSerializer[JournalRow] = new ByteArrayJournalSerializer(
    serialization
  )

  override protected val repository: JournalRowRepository =
    new JournalRowRepositoryOnKinesis(streamName, regions, numOfShards)

}
