package se.kth.id2203.epfd

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start, ComponentDefinition => _, Port => _}

class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect];
  indication[Restore];
}

case class StartEPFD(topology: Set[NetAddress]) extends KompicsEvent;
case class Suspect(src: NetAddress) extends KompicsEvent;
case class Restore(src: NetAddress) extends KompicsEvent;
case class HeartbeatReply(seq: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int) extends KompicsEvent;
case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

class EPFD() extends ComponentDefinition {

  //EPFD subscriptions
  val timer: PositivePort[Timer] = requires[Timer];
  val pLink: PositivePort[Network] = requires[Network];
  val epfd: NegativePort[EventuallyPerfectFailureDetector] = provides[EventuallyPerfectFailureDetector];

  //configuration parameters
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var topology: List[NetAddress] = List[NetAddress]();
  val delta: Long = cfg.getValue[Long]("id2203.project.delay");

  //mutable state
  var delay: Long = cfg.getValue[Long]("id2203.project.delay");
  var alive: Set[NetAddress] = Set[NetAddress]();
  var suspected: Set[NetAddress] = Set[NetAddress]();
  var seqnum = 0;

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(delay);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  //EPFD event handlers
  epfd uponEvent {
    case StartEPFD(topology) => {
      this.topology = topology.toList;
      this.alive = topology;
      startTimer(delay);
    }
  }

  timer uponEvent {
    case CheckTimeout(_) =>  {
      if (!alive.intersect(suspected).isEmpty) {
        delay += delta;
      }

      seqnum = seqnum + 1;

      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected = suspected + p;
          trigger(Suspect(p) -> epfd);
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          trigger(Restore(p) -> epfd);
        }
        trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> pLink);
      }
      alive = Set[NetAddress]();
      startTimer(delay);
    }
  }

  pLink uponEvent {
    case NetMessage(header, HeartbeatRequest(seq)) =>  {
      trigger(NetMessage(self, header.src, HeartbeatReply(seq)) -> pLink);
    }
    case NetMessage(header, HeartbeatReply(seq)) => {
      if ((seq == seqnum) || (suspected.contains(header.src))) {
        alive = alive + header.src;
      }
    }
  }
}
