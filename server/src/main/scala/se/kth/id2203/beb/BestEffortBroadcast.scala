package se.kth.id2203.beb

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}

import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver];
  request[BEB_Broadcast];
}

case class BEB_Deliver(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
case class BEB_Topology(topology: Set[NetAddress]) extends KompicsEvent;

class BasicBroadcast() extends ComponentDefinition {

  //BasicBroadcast Subscriptions
  val pLink: PositivePort[Network] = requires[Network];
  val beb: NegativePort[BestEffortBroadcast] = provides[BestEffortBroadcast];
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var topology: Set[NetAddress] = Set[NetAddress]();

  //BasicBroadcast Event Handlers
  beb uponEvent {
    case x: BEB_Broadcast => {
      for (q <- topology) {
        trigger(NetMessage(self, q, x) -> pLink);
      }
    }
    case BEB_Topology(topology) => {
      this.topology = topology;
    }
  }

  pLink uponEvent {
    case NetMessage(header, BEB_Broadcast(payload)) => {
      trigger(BEB_Deliver(header.src, payload) -> beb);
    }
  }
}