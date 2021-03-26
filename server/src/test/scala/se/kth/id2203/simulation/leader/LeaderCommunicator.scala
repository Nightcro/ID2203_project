package se.kth.id2203.simulation.leader

import se.kth.id2203.ble.{BLE_Leader, BallotLeaderElection, StartElection}
import se.kth.id2203.bootstrapping.{Booted, Bootstrapping}
import se.kth.id2203.kvservice.{Get, OpCode}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer

case class LeaderElected(leaderNo: Int, leader: NetAddress) extends KompicsEvent;

class LeaderCommunicator extends ComponentDefinition {
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection];
  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping);
  val timer: PositivePort[Timer] = requires[Timer];
  val net: PositivePort[Network] = requires[Network];

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val target: NetAddress = cfg.getValue[NetAddress]("id2203.project.target");
  var leaderNo: Int = 0;

  boot uponEvent {
    case Booted(assignment: LookupTable) => {
      val currentPartition = assignment.findPartitionForNetAddress(self);
      currentPartition match {
        case Some((_, cp)) =>
          trigger(StartElection(cp.toSet) -> ble);
        case None =>
          log.warn("Current partition not found");
      }
    }
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
      trigger(NetMessage(self, target, LeaderElected(leaderNo, l)) -> net);
      leaderNo = leaderNo + 1;
    }
  }
}