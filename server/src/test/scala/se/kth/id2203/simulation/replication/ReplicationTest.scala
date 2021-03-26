package se.kth.id2203.simulation.replication

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import se.kth.id2203.ParentComponent
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.network.impl.NetworkModels
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._

import java.net.{InetAddress, UnknownHostException}
import scala.concurrent.duration._


class ReplicationTest extends AnyFlatSpec with Matchers {
  private val nMessages = 10;

  "Replication" should "be implemented" in {
    val seed = 123L;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = LeaderTestScenario.scenario(10);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    simpleBootScenario.simulate(classOf[LauncherComp]);

    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"unit_test$i").get should be (s"kth$i");
    }
  }
}

object LeaderTestScenario {

  import Distributions._
  // needed for the distributions, but needs to be initialised after setting the seed
  implicit val random = JSimulationScenario.getRandom();

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1;

  val setUniformLatencyNetwork = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withUniformRandomDelay(5, 9)));

  val startServerOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr);
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1))
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };

  val startClientPutOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(1)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClientPut], conf);
  };

  val startClientGetOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(2)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClientGet], conf);
  };

  val killLeader4Op = Op { (self: Integer) =>
    KillNode(intToServerAddress(4));
  };

  def scenario(servers: Int): JSimulationScenario = {

    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0));
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClientPut = raise(1, startClientPutOp, 1.toN).arrival(constant(1.second));
    val startClientGet = raise(1, startClientGetOp, 1.toN).arrival(constant(1.second));
    val killLeader4 = raise(1, killLeader4Op, 1.toN).arrival(constant(1.seconds))

    networkSetup andThen
      0.seconds afterTermination startCluster andThen
      10.seconds afterTermination startClientPut andThen
      20.seconds afterTermination killLeader4 andThen
      10.seconds afterTermination startClientGet andThen
      100.seconds afterTermination Terminate
  }
}
