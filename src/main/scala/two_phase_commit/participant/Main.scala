package two_phase_commit.participant

import org.apache.log4j.BasicConfigurator

object Main {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val Seq(hostPort, id, partySize) = args.toSeq

    val participant = Participant(hostPort, "/2fc", id.toInt, partySize.toInt)

    try {
      participant.enter()
      participant.start()
    } catch {
      case e: Exception => println("Exception in Participant: " + e)
    }
  }
}
