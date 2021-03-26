package se.kth.id2203.simulation.linearizability

import se.kth.id2203.ble.{BLE_Leader, BallotLeaderElection}
import se.kth.id2203.bootstrapping.{Booted, Bootstrapping, GetInitialAssignments, InitialAssignments}
import se.kth.id2203.kvservice.{Cas, Get, Operation, Promote, Put}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.{LookupTable, Routing}
import se.kth.id2203.paxos.{SC_Decide, SC_Propose, SequenceConsensus}
import se.sics.kompics.sl._
import se.sics.kompics.network.Network
import se.sics.kompics.sl.simulator.SimulationResult

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable;

class KVServiceMock extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val route: PositivePort[Routing.type] = requires(Routing);
  val sc: PositivePort[SequenceConsensus] = requires[SequenceConsensus];
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection];
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");

  private val pending = mutable.Map.empty[UUID, String];
  val trace: mutable.Queue[Operation] = mutable.Queue.empty[Operation];
  var qMsgID: UUID = UUID.randomUUID();
  var leader: Option[NetAddress] = None;

  private def sendAndLog(op: Operation): Unit = {
    trigger(SC_Propose(op) -> sc);
    pending += (op.id -> op.key);
    trace.enqueue(op);
    logger.info("Sending {}", op);
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
      leader = Some(l);
    }
  }

  net uponEvent {
    case NetMessage(_, op: Operation) => {
      if (op.key.equals("start")) {
        start();
      }
    }
  }

  def start() {
    val messages = SimulationResult[Int]("messages");

    for (i <- 0 to messages) {
      sendAndLog(Put(s"unit_test$i", s"kth$i", self));
    }

    for (i <- 0 to messages) {
      sendAndLog(Get(s"unit_test$i", self));
    }

    for (i <- 0 to messages) {
      if (i % 2 == 0) {
        sendAndLog(Cas(s"unit_test$i", s"kth$i", s"cas$i", self))
      };
    }

    for (i <- 0 to messages) {
      val op = Get(s"unit_test$i", self);
      qMsgID = op.id;
      sendAndLog(op);
    }
  }

  sc uponEvent {
    case SC_Decide(op) => {
      var correctTrace = true;

      if(op.id.equals(qMsgID)){
        val tempTrace = trace.clone();
        val opr = tempTrace.dequeue();

        while(!opr.id.equals(trace.dequeue().id))
        if(trace.nonEmpty){
          correctTrace = false;
        }

        SimulationResult += ("Linearizability" -> correctTrace);
      }
    }
  }
}
