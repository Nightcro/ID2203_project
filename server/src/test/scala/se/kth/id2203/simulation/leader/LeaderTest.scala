package se.kth.id2203.simulation.leader

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import se.kth.id2203.ParentComponent
import se.kth.id2203.networking._
import se.kth.id2203.simulation.linearizability.LinearizabilityTestScenario.intToServerAddress
import se.kth.id2203.simulation.linearizability.ScenarioClientLinearizabilityStart
import se.kth.id2203.simulation.operations.ScenarioClientGet
import se.kth.id2203.simulation.operations.SimpleScenario.{intToClientAddress, intToServerAddress, startClientOpPut}
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.network.impl.NetworkModels
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._

import java.net.{InetAddress, UnknownHostException}
import scala.concurrent.duration._


class LeaderTest extends AnyFlatSpec with Matchers {
  "Leader election" should "be implemented" in {
    val seed = 123L;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = LeaderTestScenario.scenario(10);
    val res = SimulationResultSingleton.getInstance();
    simpleBootScenario.simulate(classOf[LauncherComp]);

    for (i <- 0 to 3) {
      SimulationResult.get[String](s"Leader$i").get should startWith ("/192.193.0.");
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

  val setUniformLatencyNetwork = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withConstantDelay(0)));

  val startServerOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr,
          "id2203.project.target" -> intToClientAddress(1));
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1),
        "id2203.project.target" -> intToClientAddress(1))
    };
    StartNode(selfAddr, Init.none[LeaderParentComponentMock], conf);
  };

  val startClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr);
    StartNode(selfAddr, Init.none[ScenarioClientLeaderStart], conf);
  };

  val killLeader4Op = Op { (self: Integer) =>
    KillNode(intToServerAddress(4));
  };

  val killLeader3Op = Op { (self: Integer) =>
    KillNode(intToServerAddress(3));
  };

  val killLeader6Op = Op { (self: Integer) =>
    KillNode(intToServerAddress(6));
  };

  def scenario(servers: Int): JSimulationScenario = {

    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0));
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClient = raise(1, startClientOp, 1.toN).arrival(constant(1.second));
    val killLeader4 = raise(1, killLeader4Op, 1.toN).arrival(constant(1.seconds))
    val killLeader3 = raise(1, killLeader3Op, 1.toN).arrival(constant(1.seconds))
    val killLeader6 = raise(1, killLeader6Op, 1.toN).arrival(constant(1.seconds))

    networkSetup andThen
      0.seconds afterTermination startCluster andThen
      0.seconds afterTermination startClient andThen
      20.seconds afterTermination killLeader4 andThen
      20.seconds afterTermination killLeader3 andThen
      20.seconds afterTermination killLeader6 andThen
      200.seconds afterTermination Terminate
  }
}
