package akka.persistence.journal

import akka.actor.ActorRef
import akka.persistence.JournalProtocol.{ WriteMessages, WriteMessagesSuccessful }
import akka.persistence.{ AtomicWrite, Persistence, PersistentRepr }
import akka.testkit.TestProbe

class WriteJournalUtils(extension: Persistence) {

  protected val actorInstanceId = 1

  def journal: ActorRef =
    extension.journalFor(null)

  def supportsAtomicPersistAllOfSeveralEvents: Boolean = true

  def expectMsg(probe: TestProbe, msg: Any) =
    probe.expectMsg(WriteMessagesSuccessful)

  def expectWriteMessagesSuccessful(probe: TestProbe) =
    expectMsg(probe, WriteMessagesSuccessful)

  def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {

    def persistentRepr(sequenceNr: Long) =
      PersistentRepr(payload = s"a-$sequenceNr",
                     sequenceNr = sequenceNr,
                     persistenceId = pid,
                     sender = sender,
                     writerUuid = writerUuid)

    val msgs = if (supportsAtomicPersistAllOfSeveralEvents) {
      (fromSnr until toSnr).map { i ⇒
        if (i == toSnr - 1)
          AtomicWrite(List(persistentRepr(i), persistentRepr(i + 1)))
        else
          AtomicWrite(persistentRepr(i))
      }
    } else {
      (fromSnr to toSnr).map { i ⇒
        AtomicWrite(persistentRepr(i))
      }
    }

    journal ! WriteMessages(msgs, sender, actorInstanceId)

  }

}
