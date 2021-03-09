package se.kth.id2203.fifo

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break

class FIFOPlink extends Port {
  indication[PL_Send];
  request[PL_Deliver];
}

case class PL_Send(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class PL_Deliver(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;

case class Tuple(payload: KompicsEvent, i: Integer) extends KompicsEvent;

class FIFO extends ComponentDefinition {

  val fifoPlink: NegativePort[FIFOPlink] = provides[FIFOPlink];
  val pLink: PositivePort[Network] = requires[Network];

  val lsn: mutable.Map[NetAddress, Integer] = mutable.HashMap.empty[NetAddress, Integer].withDefaultValue(0);
  val next: mutable.Map[NetAddress, Integer] = mutable.HashMap.empty[NetAddress, Integer].withDefaultValue(1);
  val pending: ListBuffer[(NetAddress, KompicsEvent, Integer)] = ListBuffer.empty;;

  fifoPlink uponEvent {
    case PL_Send(q, m) => {
      lsn(q) = lsn(q) + 1;
      trigger(PL_Send(q, Tuple(m, lsn(q))) -> pLink);
    }
  }

  pLink uponEvent {
    case PL_Send(p, Tuple(m, i)) => {
      pending += (p, m, i);

      while (true) {
        val ceva = pending.find(x => next.contains(x._1));
        if (ceva.isEmpty) {
          break;
        }
        val (q, n, sn) = ceva.get;
        next(q) = next(q) + 1;
        pending -= ceva.get;
        trigger(PL_Deliver(q, n) -> fifoPlink);
      }
    }
  }
}

