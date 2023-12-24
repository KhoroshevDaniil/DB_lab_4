package two_phase_commit.participant

import org.apache.log4j.{Level, Logger}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import scala.util.Random

case class Participant(hostPort: String, root: String, id: Int, partySize: Int) extends Watcher {
  private val log = Logger.getLogger(id.toString)
  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()

  private val node = s"participant_$id"
  private val path = s"$root/$node"

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notifyAll()
    }
  }

  def enter(): Boolean = {
    log.setLevel(Level.INFO)
    // код создания узла и ожидания у барьера
    zk.create(path, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for other participants.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
    }
    return false
  }

  def start(): Unit = {
    mutex.synchronized {
      val status = if (Random.nextBoolean()) "commit" else "rollback"
      zk.setData(path, status.getBytes(),-1)
      log.info(s"Sent status to Coordinator: $status")

      log.info("Waiting for result from Coordinator")
      var gotResponse = false
      var result = ""
      while (!gotResponse) {
        result = new String(zk.getData(path, this, null))
        log.info(s"CURRENT RESULT: $result")
        if (result.nonEmpty && result.startsWith("result:")) {
          gotResponse = true
        }
        else {
          Thread.sleep(1000)
        }
      }
      log.info(s"Result from Coordinator: $result")
      zk.close()
    }
  }

}
