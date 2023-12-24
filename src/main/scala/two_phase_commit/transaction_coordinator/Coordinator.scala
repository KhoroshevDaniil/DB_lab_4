package two_phase_commit.transaction_coordinator

import org.apache.log4j.{Level, Logger}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}


import scala.collection.mutable

case class Coordinator(hostPort: String, root: String, partySize: Int) extends Watcher {
  private val log = Logger.getLogger("coordinator")

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()

  private val statuses: mutable.Map[String, String] = mutable.Map()

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notifyAll()
    }
  }

  def enter(): Boolean = {
    log.setLevel(Level.INFO)
    // код создания узла и ожидания у барьера

    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for all participants.")
          mutex.wait()
          println("Noticed someone.")
        } else {
//          mutex.notifyAll()
          return true
        }
      }
    }
    return false
  }

  def start(): Unit = {
    mutex.synchronized {
      while (statuses.size < partySize) {
        val participants = zk.getChildren(root, this)
        log.info(s"PARTICIPANTS: ${participants.toString}")
        participants.forEach(participant => {
          if (!statuses.keys.exists(_ == participant)) {
            val status = new String(zk.getData(s"$root/$participant", this, null))
            if (status == "commit" || status == "rollback") {
              statuses.put(participant, status)
              log.info(s"Got status from $participant: $status")
            }
          }
        })
        if (statuses.size < partySize) {
          println("Waiting for all statuses")
          mutex.wait()
        }
      }
      log.info(s"STATUSES: ${statuses.toString()}")
      val result = if (statuses.count(s => s._2 == "rollback") > 0) "result:rollback" else "result:commit"
      val participants = zk.getChildren(root, this)
      participants.forEach(participant => {
        zk.setData(s"$root/$participant", result.getBytes(), -1)
      })
      log.info(s"Result: $result")
      zk.close()
    }
  }
}
