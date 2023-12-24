package spaghetti

import org.apache.log4j.BasicConfigurator

object Main {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val Seq(hostPort, id, partySize) = args.toSeq

    val philosopher = Philosopher(hostPort, "/table", id.toInt, partySize.toInt, lifeTime = 100000)

    try {
      philosopher.enter()
      philosopher.start()
    } catch {
      case e: Exception => println("Exception in Philosopher: " + e)
    }
  }
}
