package se.kth.id2203.simulation.linearizability

import se.kth.id2203.ble.{BLE_Leader, BallotLeaderElection, CheckTimeout, StartElection}
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, NegativePort, PositivePort}
import se.sics.kompics.timer.{ScheduleTimeout, Timer}

class GossipLeaderElectionMock() extends ComponentDefinition {
  val ble: NegativePort[BallotLeaderElection] = provides[BallotLeaderElection];
  val pl: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];

  val leader: NetAddress = cfg.getValue[NetAddress]("id2203.project.leader")
  private val ballotLeader = ballotFromNAddress(0, leader);
  private val period = cfg.getValue[Long]("id2203.project.delayStart");

  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  private def startTimer(): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  timer uponEvent {
    case CheckTimeout(_) => {
      trigger(BLE_Leader(leader, ballotLeader) -> ble);
    }
  }

  ble uponEvent {
    case StartElection(nodes: Set[NetAddress]) => {
      startTimer();
    }
  }
}
