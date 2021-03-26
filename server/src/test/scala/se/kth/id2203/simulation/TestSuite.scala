package se.kth.id2203.simulation

import org.scalatest.Suites
import se.kth.id2203.simulation.leader.LeaderTest
import se.kth.id2203.simulation.linearizability.LinearizabilityTest
import se.kth.id2203.simulation.operations.OpsTest
import se.kth.id2203.simulation.replication.ReplicationTest

class TestSuite extends Suites (new ReplicationTest, new OpsTest, new LinearizabilityTest, new LeaderTest)

