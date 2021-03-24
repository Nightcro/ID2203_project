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
package se.kth.id2203.kvservice;

import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.kth.id2203.paxos
import se.kth.id2203.paxos.Role.{FOLLOWER, LEADER, Role}
import se.kth.id2203.paxos.{SC_Decide, SC_Propose, SequenceConsensus}
import se.sics.kompics.sl._
import se.sics.kompics.network.Network

import scala.collection.concurrent.TrieMap;

case class Promote(rl: Role) extends KompicsEvent;

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val route: PositivePort[Routing.type] = requires(Routing);
  val sc: PositivePort[SequenceConsensus] = requires[SequenceConsensus];
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val keyValueMap = TrieMap.empty[String, String];
  var state: paxos.Role.Value = FOLLOWER;

  //******* Handlers ******
  net uponEvent {
    case NetMessage(_, op: Operation) => {
      log.info("Propose {} {}", op, op.key);
      trigger(SC_Propose(op) -> sc);
    }
    case NetMessage(_, Promote(rl)) => {
      state = rl;
    }
  }

  sc uponEvent {
    case SC_Decide(op: Get) => {
      log.info("GET operation {}", op);
      val value = if (keyValueMap.contains(op.key)) Some(keyValueMap(op.key)) else None

      state match {
        case LEADER => {
          log.info("Leader responding to {}", op.src);
          trigger(NetMessage(self, op.src, op.response(OpCode.Ok, value)) -> net);
        }
        case FOLLOWER => {
          log.info("Not leader, just recording the commit");
        }
      }
    }
    case SC_Decide(op: Put) => {
      log.info("PUT operation {}", op);
      keyValueMap += ((op.key, op.value))

      state match {
        case LEADER => {
          log.info("Leader responding to {}", op.src);
          trigger(NetMessage(self, op.src, op.response(OpCode.Ok, Some(op.value))) -> net);
        }
        case FOLLOWER => {
          log.info("Not leader, just recording the commit");
        }
      }
    }
    case SC_Decide(op: Cas) => {
      log.info("CAS operation {}", op);
      val value: Option[String] = if (keyValueMap.contains(op.key)) Some(keyValueMap(op.key)) else None
      if (value.isDefined && (value.get == op.referenceValue)) {
        keyValueMap += ((op.key, op.newValue))
      }

      state match {
        case LEADER => {
          log.info("Leader responding to {}", op.src);
          trigger(NetMessage(self, op.src, op.response(OpCode.Ok, value)) -> net)
        }
        case FOLLOWER => {
          log.info("Not leader, just recording the commit");
        }
      }
    }
  }
}
