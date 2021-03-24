package se.kth.id2203.simulation.linearizability

import se.kth.id2203.kvservice.{Cas, Get, OpResponse, Operation, Put}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import java.util.UUID
import scala.collection.mutable

class ScenarioClientStart extends ComponentDefinition {
  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val target: NetAddress = cfg.getValue[NetAddress]("id2203.project.target");

  ctrl uponEvent {
    case _: Start => {
      val op = Put(s"start", s"start", self);
      trigger(NetMessage(self, target, op) -> net);
    }
  }
}
