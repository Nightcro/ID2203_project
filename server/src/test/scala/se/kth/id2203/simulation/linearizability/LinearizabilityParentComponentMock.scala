package se.kth.id2203.simulation.linearizability

import se.kth.id2203.ble.{BallotLeaderElection, GossipLeaderElection}
import se.kth.id2203.bootstrapping.{BootstrapClient, BootstrapServer, Bootstrapping, GetInitialAssignments, InitialAssignments}
import se.kth.id2203.fifo.{FIFO, FIFOPlink}
import se.kth.id2203.kvservice.KVService
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay.{Routing, VSOverlayManager}
import se.kth.id2203.paxos.{SequenceConsensus, SequencePaxos}
import se.sics.kompics.{Component, Init}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort}
import se.sics.kompics.timer.Timer

import java.net.InetAddress

class LinearizabilityParentComponentMock extends ComponentDefinition {
  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  //******* Children ******
  val fifoPlink: Component = create(classOf[FIFO], Init.NONE);
  val ble: Component = create(classOf[GossipLeaderElectionMock], Init.NONE);
  val sc: Component = create(classOf[SequencePaxos], Init.NONE);
  val overlay: Component = create(classOf[VSOverlayManager], Init.NONE);
  val kvBeta: Component = create(classOf[KVServiceMock], Init.NONE);
  val kvReal: Component = create(classOf[KVService], Init.NONE);
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val target: NetAddress = cfg.getValue[NetAddress]("id2203.project.target");
  val boot: Component = cfg.readValue[NetAddress]("id2203.project.bootstrap-address") match {
    case Some(_) => create(classOf[BootstrapClient], Init.NONE); // start in client mode
    case None    => create(classOf[BootstrapServer], Init.NONE); // start in server mode
  }

  {
    // Boot
    connect[Timer](timer -> boot);
    connect[Network](net -> boot);

    // FIFO
    connect[Network](net -> fifoPlink);

    // Overlay
    connect(Bootstrapping)(boot -> overlay);
    connect[Network](net -> overlay);
    connect[SequenceConsensus](sc -> overlay);
    connect[BallotLeaderElection](ble -> overlay);

    // KV
    if (self.getIp().equals(target.getIp())) {
      connect(Routing)(overlay -> kvBeta);
      connect[Network](net -> kvBeta);
      connect[SequenceConsensus](sc -> kvBeta);
      connect[BallotLeaderElection](ble -> kvBeta);
    } else {
      connect(Routing)(overlay -> kvReal);
      connect[Network](net -> kvReal);
      connect[SequenceConsensus](sc -> kvReal);
    }

    // BLE
    connect[Timer](timer -> ble);
    connect[Network](net -> ble);

    // Sequence Paxos
    connect[Network](net -> sc);
    connect[FIFOPlink](fifoPlink -> sc);
    connect[BallotLeaderElection](ble -> sc);
  }
}
