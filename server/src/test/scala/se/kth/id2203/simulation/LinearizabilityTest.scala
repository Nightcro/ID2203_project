package se.kth.id2203.simulation

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


class LinearizabilityTest extends AnyFlatSpec with Matchers {
    private val nMessages = 4;
    private val clusterSize = 9;

    "Linearizability" should "be implemented" in {
      val seed = 123l
      JSimulationScenario.setSeed(seed)
      val simpleBootScenario = LinearizabilityTestScenario.scenario(clusterSize);
      val res = SimulationResultSingleton.getInstance()
      SimulationResult += ("messages" -> nMessages)
      simpleBootScenario.simulate(classOf[LauncherComp])
      for (i <- 0 to nMessages) {
        //PUT operation
        SimulationResult.get[String](s"unit_test$i") should be (Some(s"kth$i"))
        //GET operation
        SimulationResult.get[String](s"unit_test$i") should be (Some(s"kth$i"))
      }
      for(i <- 0 to nMessages/2){
        SimulationResult.get[String](s"unit_test$i") should be (Some(s"kth$i"))
      }

    }
  }

  object LinearizabilityTestScenario {

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
        Map("id2203.project.address" -> selfAddr)
      } else {
        Map(
          "id2203.project.address" -> selfAddr,
          "id2203.project.bootstrap-address" -> intToServerAddress(1))
      };
      StartNode(selfAddr, Init.none[ParentComponent], conf);
    };

    val startClientOp = Op { (self: Integer) =>
      val selfAddr = intToClientAddress(self)
      val conf = Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1));
      StartNode(selfAddr, Init.none[LinearizabilityTestClient], conf);
    };

    def scenario(servers: Int): JSimulationScenario = {

      val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0));
      val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
      val startClients = raise(1, startClientOp, 1.toN).arrival(constant(1.second));

      networkSetup andThen
        0.seconds afterTermination startCluster andThen
        10.seconds afterTermination startClients andThen
        100.seconds afterTermination Terminate
    }
}
