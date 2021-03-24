package se.kth.id2203.ble

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.KompicsEvent

import scala.collection.mutable

class BallotLeaderElection extends Port {
  indication[BLE_Leader];
  indication[BLE_Topology];
}

case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;
case class BLE_Topology(topology: Set[NetAddress]) extends KompicsEvent;

case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);
case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;
case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;
case class StartElection(nodes: Set[NetAddress]) extends KompicsEvent;

class GossipLeaderElection() extends ComponentDefinition {

  val ble: NegativePort[BallotLeaderElection] = provides[BallotLeaderElection];
  val pl: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  var topology: Set[NetAddress] = Set.empty;
  val delta: Long = cfg.getValue[Long]("id2203.project.delay");

  private var period = cfg.getValue[Long]("id2203.project.delay");
  private val ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0L;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;

  private val ballotOne = 0x0100000000L;

  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

  ble uponEvent {
    case StartElection(nodes: Set[NetAddress]) => {
      topology = nodes;
      startTimer();
    }
  }

  private def startTimer(): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  private def checkLeader(): Unit = {
    val (topProcess, topBallot) = (ballots + ((self, ballot))).maxBy(_._2);

    if (topBallot < highestBallot) {
      while (ballot <= highestBallot) {
        ballot = incrementBallot(ballot);
      }
      leader = None;
    } else {
      if (!leader.contains((topBallot, topProcess))) {
        highestBallot = topBallot;
        leader = Some((topBallot, topProcess));
        log.info("Sending leader {}", topProcess);
        trigger(BLE_Leader(topProcess, topBallot) -> ble);
      }

      val topology = ballots.keys.toSet + self;
      trigger(BLE_Topology(topology) -> ble);
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => {
      if (ballots.size + 1 >= (topology.size / 2)) {
        checkLeader();
      }

      ballots.clear();
      round = round + 1;

      for (p <- topology) {
        if (p != self) {
          trigger(NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> pl);
        }
      }

      startTimer();
    }
  }

  pl uponEvent {
    case NetMessage(header, HeartbeatReq(r, hb)) => {
      if (hb > highestBallot) {
        highestBallot = hb;
      }

      trigger(NetMessage(self, header.src, HeartbeatResp(r, ballot)) -> pl);
    }
    case NetMessage(header, HeartbeatResp(r, b)) => {
      if (r == round) {
        ballots += ((header.src, b));
      } else {
        period = period + delta;
      }
    }
  }
}
