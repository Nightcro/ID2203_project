/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay;

import se.kth.id2203.ble.{BLE_Leader, BallotLeaderElection}
import se.kth.id2203.bootstrapping._
import se.kth.id2203.fifo.FIFOPlink
import se.kth.id2203.networking._
import se.kth.id2203.paxos.{SequenceConsensus, StartSequenceCons}
import se.sics.kompics.sl._
import se.sics.kompics.network.Network
import se.sics.kompics.timer.Timer

import util.Random;

/**
  * The V(ery)S(imple)OverlayManager.
  * <p>
  * Keeps all nodes in a single partition in one replication group.
  * <p>
  * Note: This implementation does not fulfill the project task. You have to
  * support multiple partitions!
  * <p>
  * @author Lars Kroll <lkroll@kth.se>
  */
class VSOverlayManager extends ComponentDefinition {

  //******* Ports ******
  val route: NegativePort[Routing.type] = provides(Routing);
  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping);
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  var seqCons: PositivePort[SequenceConsensus] = requires[SequenceConsensus];
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection];
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  private var lut: Option[LookupTable] = None;
  //******* Handlers ******
  boot uponEvent {
    case GetInitialAssignments(nodes) => {
      log.info("Generating LookupTable...");
      val lut = LookupTable.generate(nodes);
      log.debug("Generated assignments:\n{}", lut);
      trigger(new InitialAssignments(lut) -> boot);
    }
    case Booted(assignment: LookupTable) => {
      log.info("Got NodeAssignment, overlay ready.");
      lut = Some(assignment);
      val currentPartition = assignment.findPartitionForNetAddress(self);
      assignment.setCurrentPartition(self);
      currentPartition match {
        case Some((_, cp)) =>
          trigger(StartSequenceCons(cp.toSet) -> seqCons);
        case None =>
          log.warn("Current partition not found");
      }
    }
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
      lut.get.setLeaderPartition(l);
    }
  }

  net uponEvent {
    case NetMessage(header, RouteMsg(key, msg)) => {
      val target = lut.get.getLeaderCurrentPartition();
      log.info(s"Forwarding message for key $key to $target");
      trigger(NetMessage(header.src, target, msg) -> net);
    }
    case NetMessage(header, msg: Connect) => {
      lut match {
        case Some(l) => {
          log.debug("Accepting connection request from {}", header.src);
          val size = l.getNodes().size;
          trigger(NetMessage(self, header.src, msg.ack(size)) -> net);
        }
        case None => log.info("Rejecting connection request from {}, as system is not ready, yet.", header.src);
      }
    }
  }

  route uponEvent {
    case RouteMsg(key, msg) => {
      val nodes = lut.get.lookup(key);
      assert(nodes.nonEmpty);
      val i = Random.nextInt(nodes.size);
      val target = nodes.drop(i).head;
      log.info(s"Routing message for key $key to $target");
      trigger(NetMessage(self, target, msg) -> net);
    }
  }
}
