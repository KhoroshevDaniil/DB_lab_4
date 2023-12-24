package two_phase_commit.transaction_coordinator

import org.apache.log4j.BasicConfigurator

object Main {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val Seq(hostPort, partySize) = args.toSeq

    val coordinator = Coordinator(hostPort, "/2fc", partySize.toInt)
    try {
      coordinator.enter()
      coordinator.start()
    } catch {
      case e: Exception => println("Exception in Coordinator: " + e)
    }
  }
}
