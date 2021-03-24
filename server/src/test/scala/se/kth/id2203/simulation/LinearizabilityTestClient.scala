package se.kth.id2203.simulation

import se.kth.id2203.kvservice._
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import java.util.UUID
import scala.collection.mutable;

class LinearizabilityTestClient extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network]
  val timer = requires[Timer]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address")
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address")
  private val pending = mutable.Map.empty[UUID, String]
  val trace = mutable.Queue.empty[Operation]
  var traceNo = 0;
  var oP : Operation = Cas("", "", "", self);
  var qMsgID = oP.id
  ctrl uponEvent {
    case _: Start => {
      val messages = SimulationResult[Int]("messages")
      for (i <- 0 to messages) {
        val opPUT = new Put(s"unit_test$i", s"kth$i", self);
        val routeMsg = RouteMsg(opPUT.key, opPUT) // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(NetMessage(self, server, routeMsg) -> net)
        trace.enqueue(opPUT)
        pending += (opPUT.id -> opPUT.key)
        logger.info("Sending {}", opPUT)
        SimulationResult += (opPUT.key -> "Sent")

        val opGet = new Get( s"unit_test$i", self);
        val routeMsg1 = RouteMsg(opGet.key, opGet) // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(NetMessage(self, server, routeMsg1) -> net)
        trace.enqueue(opGet)
        pending += (opGet.id -> opGet.key)
        logger.info("Sending {}", opGet)
        SimulationResult += (opGet.key -> "Sent")
      }
      for(i <- 0 to messages/2) {
        val op = new Cas(s"unit_test$i", s"kth$i", s"cas$i", self)
        qMsgID = op.id
        val routeMsg = RouteMsg(op.key, op)
        trigger(NetMessage(self, server, routeMsg) -> net)
      }
    }
  }

  net uponEvent {
    case NetMessage(header, or @ OpResponse(id, status, res)) => {
      logger.debug(s"Got OpResponse: $or")
      var correctTrace = true
      if(id.equals(qMsgID)){
        val tempTrace = trace.clone()
        for(i <- 0 to SimulationResult[Int]("messages")*2){
          val opr = tempTrace.dequeue()
          while(!opr.id.equals(trace.dequeue().id)){
          }
        }
        if(trace.isEmpty){
          correctTrace = false
          log.info("Not Linearizable")
        }
        else{
          log.info("Is Linearizable")
        }
        traceNo = traceNo + 1
        SimulationResult += (traceNo.toString + self.toString -> correctTrace)
      }
    }
  }
}