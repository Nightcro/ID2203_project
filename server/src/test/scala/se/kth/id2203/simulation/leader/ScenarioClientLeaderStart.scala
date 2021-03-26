package se.kth.id2203.simulation.leader

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer

class ScenarioClientLeaderStart extends ComponentDefinition {
  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];

  net uponEvent {
    case NetMessage(header, LeaderElected(i, l)) => {
      SimulationResult += (s"Leader$i" -> l.getIp().toString);
    }
  }
}
