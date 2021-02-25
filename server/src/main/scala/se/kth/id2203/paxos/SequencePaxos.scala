package se.kth.id2203.paxos

import se.kth.id2203.ble.{BLE_Leader, BallotLeaderElection, StartElection}
import se.kth.id2203.kvservice.Op
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
case class Promise(nL: Long, na: Long, suffix: List[Op], ld: Int) extends KompicsEvent;
case class AcceptSync(nL: Long, suffix: List[Op], ld: Int) extends KompicsEvent;
case class Accept(nL: Long, c: Op) extends KompicsEvent;
case class Accepted(nL: Long, m: Int) extends KompicsEvent;
case class Decide(ld: Int, nL: Long) extends KompicsEvent;
case class SC_Propose(value: Op) extends KompicsEvent;
case class SC_Decide(value: Op) extends KompicsEvent;

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
  val pl: PositivePort[Network] = requires[Network];

  var self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var pi: Set[NetAddress] = Set[NetAddress]();
  var others: Set[NetAddress] = Set[NetAddress]();
  var majority: Int = (pi.size / 2) + 1;

  var state: (paxos.Role.Value, paxos.State.Value) = (FOLLOWER, UNKNOWN);
  var nL = 0L;
  var nProm = 0L;
  var leader: Option[NetAddress] = None;
  var na = 0L;
  var va = List.empty[Op];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Op];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[Op])];

  def suffix(s: List[Op], l: Int): List[Op] = {
    s.drop(l)
  }

  def prefix(s: List[Op], l: Int): List[Op] = {
    s.take(l)
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
      if (n > nL) {
        leader = Some(l);
        nL = n;

        if ((self == l) && (nL > nProm)) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Op];
          las.clear();
          lds.clear();
          acks.clear();
          lc = 0;

          for (p <- pi) {
            if (p != self) {
              trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> pl);
            }
          }

          acks(l) = (na, suffix(va, ld));
          lds(self) = ld;
          nProm = nL;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  pl uponEvent {
    case NetMessage(header, Prepare(np, ldp, n)) => {
      if (nProm < np) {
        nProm = np;
        state = (FOLLOWER, PREPARE);

        var sfx = List.empty[Op];
        if (na >= n) {
          sfx = suffix(va, ldp);
        }

        trigger(NetMessage(self, header.src, Promise(np, na, sfx, ld)) -> pl);
      }
    }

    case NetMessage(header, Promise(n, nac, sfxa, lda)) => {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        na = nac;
        acks(header.src) = (nac, sfxa);
        lds(header.src) = lda;
        val P = pi.filter(acks.contains);

        if (P.size == majority) {
          val (k, sfx) = acks(P.maxBy(acks(_)._1));
          va = prefix(va, ld) ++ sfx ++ propCmds;
          las(self) = va.size;
          propCmds = List.empty[Op];
          state = (LEADER, ACCEPT);

          for (p <- pi.filter(x => lds.contains(x) && (x != self))) {
            val sfxp = suffix(va, lds(p));
            trigger(NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> pl);
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds(header.src) = lda;
        val sfx = suffix(va, lds(header.src));

        trigger(NetMessage(self, header.src, AcceptSync(nL, sfx, lds(header.src))) -> pl);

        if (lc != 0) {
          trigger(NetMessage(self, header.src, Decide(ld, nL)) -> pl);
        }
      }
    }

    case NetMessage(header, AcceptSync(nL, sfx, ldp)) => {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL;
        va = prefix(va, ld) ++ sfx;

        trigger(NetMessage(self, header.src, Accepted(nL, va.size)) -> pl);

        state = (FOLLOWER, ACCEPT);
      }
    }

    case NetMessage(header, Accept(nL, c)) => {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va = va :+ c;
        trigger(NetMessage(self, header.src, Accepted(nL, va.size)) -> pl);
      }
    }

    case NetMessage(_, Decide(l, nL)) => {
      if (nProm == nL) {
        while (ld < l) {
          trigger(SC_Decide(va(ld)) -> sc);
          ld = ld + 1;
        }
      }
    }

    case NetMessage(header, Accepted(n, m)) => {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las(header.src) = m;

        if ((lc < m) && (pi.count(x => las.contains(x) && (las(x) >= m)) >= majority)) {
          lc = m;

          for (p <- pi.filter(lds.contains)) {
            trigger(NetMessage(self, p, Decide(lc, nL)) -> pl);
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
          trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
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
