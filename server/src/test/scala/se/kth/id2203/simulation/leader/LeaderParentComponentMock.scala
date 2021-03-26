package se.kth.id2203.simulation.leader

import se.kth.id2203.ble.{BallotLeaderElection, GossipLeaderElection}
import se.kth.id2203.bootstrapping.{BootstrapClient, BootstrapServer, Bootstrapping}
import se.kth.id2203.fifo.{FIFO, FIFOPlink}
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay.VSOverlayManager
import se.kth.id2203.paxos.{SequenceConsensus, SequencePaxos}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer
import se.sics.kompics.{Component, Init}

class LeaderParentComponentMock extends ComponentDefinition {
  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  //******* Children ******
  val ble: Component = create(classOf[GossipLeaderElection], Init.NONE);
  val overlay: Component = create(classOf[VSOverlayManager], Init.NONE);
  val leaderCommunicator: Component = create(classOf[LeaderCommunicator], Init.NONE);
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val boot: Component = cfg.readValue[NetAddress]("id2203.project.bootstrap-address") match {
    case Some(_) => create(classOf[BootstrapClient], Init.NONE); // start in client mode
    case None    => create(classOf[BootstrapServer], Init.NONE); // start in server mode
  }

  {
    // Boot
    connect[Timer](timer -> boot);
    connect[Network](net -> boot);

    // Overlay
    connect(Bootstrapping)(boot -> overlay);
    connect[Network](net -> overlay);
    connect[BallotLeaderElection](ble -> overlay);

    // BLE
    connect[Timer](timer -> ble);
    connect[Network](net -> ble);

    // LeaderCommunicator
    connect(Bootstrapping)(boot -> leaderCommunicator);
    connect[BallotLeaderElection](ble -> leaderCommunicator);
    connect[Timer](timer -> leaderCommunicator);
    connect[Network](net -> leaderCommunicator);
  }
}
