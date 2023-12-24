package spaghetti

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import scala.util.Random

case class Philosopher(hostPort: String, root: String, id: Int, partySize: Int, lifeTime: Int) extends Watcher {
  private val log = Logger.getLogger(id.toString)

  private val philosopher_node = s"$root/philosopher_$id"

  private val left_fork_id = s"$id"
  private val right_fork_id = s"${(id + 1) % partySize}"
  private val left_fork_node = s"$root/fork_$left_fork_id"
  private val right_fork_node = s"$root/fork_$right_fork_id"

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notifyAll()
    }
  }

  def enter(): Boolean = {
    log.setLevel(Level.INFO)
    // код создания узла и ожидания у барьера
    zk.create(philosopher_node, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for the other philosophers.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
    }
    return false
  }

  private def checkAvailableForks(): Boolean = {
    val children = zk.getChildren(root, this)
    if (children.contains(s"fork_$left_fork_id") || children.contains(s"fork_$right_fork_id") ) {
      return false;
    }
    log.info(s"Available: ${right_fork_node} and ${left_fork_node}")
    true;
  }

  def start(): Unit = {
    mutex.synchronized {
      val startTime = System.currentTimeMillis();
      var delay = 0
      while (System.currentTimeMillis() < startTime + lifeTime) {
        if (checkAvailableForks()) {
          zk.create(left_fork_node, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          zk.create(right_fork_node, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

          delay = Random.nextInt(10000)
          log.info(s"Eating spaghetti for $delay")
          Thread.sleep(delay)

          zk.delete(left_fork_node, 0)
          zk.delete(right_fork_node, 0)

          delay = Random.nextInt(10000)
          log.info(s"Thinking for $delay")
          Thread.sleep(delay)
        } else {
          log.info(s"Waiting for available fork_$right_fork_id and fork_$left_fork_id")
          mutex.wait()
        }
      }
    }
  }
}
