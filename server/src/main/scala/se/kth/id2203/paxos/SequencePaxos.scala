package se.kth.id2203.paxos

import se.kth.id2203.ble.{BLE_Leader, BLE_Topology, BallotLeaderElection, StartElection}
import se.kth.id2203.fifo.{FIFO, FIFOPlink, PL_Deliver, PL_Send}
import se.kth.id2203.kvservice.{Operation, Promote}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.paxos
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.sics.kompics.KompicsEvent

import scala.collection.mutable;

class SequenceConsensus extends Port {
  request[SC_Propose];
  indication[SC_Decide];
  request[StartSequenceCons];
}

case class StartSequenceCons(nodes: Set[NetAddress]) extends KompicsEvent;
case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;
case class Promise(nL: Long, na: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;
case class AcceptSync(nL: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;
case class Accept(nL: Long, c: Operation) extends KompicsEvent;
case class Accepted(nL: Long, m: Int) extends KompicsEvent;
case class Decide(ld: Int, nL: Long) extends KompicsEvent;
case class SC_Propose(value: Operation) extends KompicsEvent;
case class SC_Decide(value: Operation) extends KompicsEvent;

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKNOWN = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}

class SequencePaxos extends ComponentDefinition {

  import State._
  import Role._

  val sc: NegativePort[SequenceConsensus] = provides[SequenceConsensus];
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection];
  val plFifo: PositivePort[FIFOPlink] = requires[FIFOPlink];
  val net: PositivePort[Network] = requires[Network];

  var self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var pi: Set[NetAddress] = Set[NetAddress]();
  var others: Set[NetAddress] = Set[NetAddress]();
  var majority: Int = (pi.size / 2) + 1;

  var state: (paxos.Role.Value, paxos.State.Value) = (FOLLOWER, UNKNOWN);
  var nL = 0L;
  var nProm = 0L;
  var leader: Option[NetAddress] = None;
  var na = 0L;
  var va = List.empty[Operation];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Operation];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[Operation])];

  def suffix(s: List[Operation], l: Int): List[Operation] = {
    s.drop(l)
  }

  def prefix(s: List[Operation], l: Int): List[Operation] = {
    s.take(l)
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
      if (n > nL) {
        leader = Some(l);
        nL = n;

        if ((self == l) && (nL > nProm)) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Operation];
          las.clear();
          lds.clear();
          acks.clear();
          lc = 0;

          for (p <- pi) {
            if (p != self) {
              trigger(PL_Send(p, Prepare(nL, ld, na)) -> plFifo);
            }
          }

          acks(l) = (na, suffix(va, ld));
          lds(self) = ld;
          nProm = nL;
          trigger(NetMessage(self, self, Promote(LEADER)) -> net);
        } else {
          state = (FOLLOWER, state._2);
          trigger(NetMessage(self, self, Promote(FOLLOWER)) -> net);
        }
      }
    }
    case BLE_Topology(topology) => {
      pi = topology;
      majority = pi.size / 2 + 1;
    }
  }

  plFifo uponEvent {
    case PL_Deliver(p, Prepare(np, ldp, n)) => {
      if (nProm < np) {
        nProm = np;
        state = (FOLLOWER, PREPARE);

        var sfx = List.empty[Operation];
        if (na >= n) {
          sfx = suffix(va, ldp);
        }

        trigger(PL_Send(p, Promise(np, na, sfx, ld)) -> plFifo);
      }
    }

    case PL_Deliver(a, Promise(n, nac, sfxa, lda)) => {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        na = nac;
        acks(a) = (nac, sfxa);
        lds(a) = lda;
        val P = pi.filter(acks.contains);

        if (P.size == majority) {
          val (k, sfx) = acks(P.maxBy(acks(_)._1));
          va = prefix(va, ld) ++ sfx ++ propCmds;
          las(self) = va.size;
          propCmds = List.empty[Operation];
          state = (LEADER, ACCEPT);

          for (p <- pi.filter(x => lds.contains(x) && (x != self))) {
            val sfxp = suffix(va, lds(p));
            trigger(PL_Send(p, AcceptSync(nL, sfxp, lds(p))) -> plFifo);
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds(a) = lda;
        val sfx = suffix(va, lds(a));

        trigger(PL_Send(a, AcceptSync(nL, sfx, lds(a))) -> plFifo);

        if (lc != 0) {
          trigger(PL_Send(a, Decide(ld, nL)) -> plFifo);
        }
      }
    }

    case PL_Deliver(p, AcceptSync(nL, sfx, ldp)) => {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL;
        va = prefix(va, ld) ++ sfx;

        trigger(PL_Send(p, Accepted(nL, va.size)) -> plFifo);

        state = (FOLLOWER, ACCEPT);
      }
    }

    case PL_Deliver(p, Accept(nL, c)) => {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va = va :+ c;
        trigger(PL_Send(p, Accepted(nL, va.size)) -> plFifo);
      }
    }

    case PL_Deliver(_, Decide(l, nL)) => {
      if (nProm == nL) {
        while (ld < l) {
          trigger(SC_Decide(va(ld)) -> sc);
          ld = ld + 1;
        }
      }
    }

    case PL_Deliver(a, Accepted(n, m)) => {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las(a) = m;

        if ((lc < m) && (pi.count(x => las.contains(x) && (las(x) >= m)) >= majority)) {
          lc = m;

          for (p <- pi.filter(lds.contains)) {
            trigger(PL_Send(p, Decide(lc, nL)) -> plFifo);
          }
        }
      }
    }
  }

  sc uponEvent {
    case SC_Propose(c) => {
      if (state == (LEADER, PREPARE)) {
        propCmds = propCmds :+ c;
      }
      else if (state == (LEADER, ACCEPT)) {
        va = va :+ c;
        las(self) += 1;

        for (p <- pi.filter(x => lds.contains(x) && (x != self))) {
          trigger(PL_Send(p, Accept(nL, c)) -> plFifo);
        }
      }
    }
    case StartSequenceCons(nodes: Set[NetAddress]) => {
      pi = nodes;
      majority = pi.size / 2 + 1;
      trigger(StartElection(nodes) -> ble);
    }
  }
}
