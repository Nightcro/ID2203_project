package se.kth.id2203.simulation.replication

import se.kth.id2203.kvservice.{OpResponse, Operation, Put}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer

import java.util.UUID
import scala.collection.mutable

class ScenarioClientPut extends ComponentDefinition {
  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  private val pending = mutable.Map.empty[UUID, String];
  //******* Handlers ******

  //BasicBroadcast Component State and Initialization
  private def sendAndLog(op: Operation): Unit = {
    val routeMsg = RouteMsg(op.key, op);
    trigger(NetMessage(self, server, routeMsg) -> net);
    pending += (op.id -> op.key);
    logger.info("Sending {}", op);
    SimulationResult += (op.key -> "Sent");
  }

  ctrl uponEvent {
    case _: Start => {
      val messages = SimulationResult[Int]("messages");

      for (i <- 0 to messages) {
        sendAndLog(Put(s"unit_test$i", s"kth$i", self));
      }
    }
  }
}
