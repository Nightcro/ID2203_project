package se.kth.id2203.simulation.linearizability

import se.kth.id2203.kvservice.Put
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer

class ScenarioClientLinearizabilityStart extends ComponentDefinition {
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
