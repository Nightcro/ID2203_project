package se.kth.id2203.fifo

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class FIFOPlink extends Port {
  request[PL_Send];
  indication[PL_Deliver];
}

case class PL_Send(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class PL_Deliver(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class PL_Topology(topology: Set[NetAddress]) extends KompicsEvent;

case class DataMessage(payload: KompicsEvent, i: Integer) extends KompicsEvent;

class FIFO extends ComponentDefinition {

  val fifoPlink: NegativePort[FIFOPlink] = provides[FIFOPlink];
  val pLink: PositivePort[Network] = requires[Network];

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val lsn: mutable.Map[NetAddress, Integer] = mutable.HashMap.empty[NetAddress, Integer];
  val next: mutable.Map[NetAddress, Integer] = mutable.HashMap.empty[NetAddress, Integer];
  val pending: ListBuffer[(NetAddress, KompicsEvent, Integer)] = ListBuffer.empty;;

  fifoPlink uponEvent {
    case PL_Send(q, m) => {
      lsn(q) += 1;
      trigger(NetMessage(self, q, DataMessage(m, lsn(q))) -> pLink);
    }
    case PL_Topology(topology) => {
      for (p <- topology) {
        lsn(p) = 0;
        next(p) = 1;
      }
    }
    case x => {
      log.error("Unkown FIFO message {}", x);
    }
  }

  pLink uponEvent {
    case NetMessage(header, DataMessage(m, i)) => {
      val listItem = (header.src, m, i);
      pending += listItem;
      log.debug("PL_Deliver {} {} {}", pending, lsn.toSet, next.toSet);

      breakable
      {
          while (true) {
          val item = pending.find(x => next.contains(x._1) && next(x._1) == x._3);
          if (item.isEmpty) {
            break;
          }

          val (q, n, sn) = item.get;
          next(q) += 1;
          pending -= item.get;
          trigger(PL_Deliver(q, n) -> fifoPlink);
        }
      }
    }
  }
}

