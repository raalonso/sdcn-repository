package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.PastryNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class OverlayNetworkTest {

    OverlayNetwork network;

    @Before
    public void setUp() throws Exception {
        this.network = new OverlayNetwork();
    }

    @After
    public void tearDown() throws Exception {
        this.network = null;
    }

    @Test
    public void scenario_1() throws Exception {
        System.out.println("Test Scenario 1!\n");
        int nodeId = -1;

        PastryNode node1 = new ObjectNode("master53.dit.upm.es");
        node1.setBootstrapNode(null);
        nodeId = node1.initPastry(53);
        node1.run();
        this.network.add(node1);

        PastryNode node2 = new ObjectNode("node60.dit.upm.es");
        node2.setBootstrapNode(node1);
        nodeId = node2.initPastry(60);
        node1.run();

        PastryNode node3 = new ObjectNode("node50.dit.upm.es");
        node3.setBootstrapNode(node1);
        nodeId = node3.initPastry(50);
        node1.run();

        PastryNode node4 = new ObjectNode("node65.dit.upm.es");
        node4.setBootstrapNode(node1);
        nodeId = node4.initPastry(65);
        node1.run();

        PastryNode node5 = new ObjectNode("node120.dit.upm.es");
        node5.setBootstrapNode(node1);
        nodeId = node5.initPastry(120);
        node1.run();

        PastryNode node6 = new ObjectNode("node115.dit.upm.es");
        node6.setBootstrapNode(node1);
        nodeId = node6.initPastry(115);
        node1.run();

        PastryNode node7 = new ObjectNode("node127.dit.upm.es");
        node7.setBootstrapNode(node1);
        nodeId = node7.initPastry(127);
        node1.run();

    }

    @Test
    public void scenario_2() throws Exception {
//        System.out.println("Test Scenario 2!\n");
//        int nodeId = -1;
//
//        Node node1 = new ObjectNode("master53.dit.upm.es");
//        nodeId = node1.initPastry(null, 53);
//        this.network.add(node1);
//        this.network.printOutNetwork();
//
//        Node node2 = new ObjectNode("node50.dit.upm.es");
//        nodeId = node2.initPastry(node1, 50);
//        this.network.add(node2);
//        this.network.printOutNetwork();
//
//        Node node3 = new ObjectNode("node55.dit.upm.es");
//        nodeId = node3.initPastry(node1, 55);
//        this.network.add(node3);
//        this.network.printOutNetwork();
//
//        Node node4 = new ObjectNode("node40.dit.upm.es");
//        nodeId = node4.initPastry(node1, 40);
//        this.network.add(node4);
//        this.network.printOutNetwork();
//
//        Node node5 = new ObjectNode("node60.dit.upm.es");
//        nodeId = node5.initPastry(node1, 60);
//        this.network.add(node5);
//        this.network.printOutNetwork();
//
//        Node node6 = new ObjectNode("node130.dit.upm.es");
//        nodeId = node6.initPastry(node1, 130);
//        this.network.add(node6);
//        this.network.printOutNetwork();
//
//        Node node7 = new ObjectNode("node140.dit.upm.es");
//        nodeId = node7.initPastry(node1, 140);
//        this.network.add(node7);
//        this.network.printOutNetwork();
//
//        Node node8 = new ObjectNode("node110.dit.upm.es");
//        nodeId = node8.initPastry(node1, 110);
//        this.network.add(node8);
//        this.network.printOutNetwork();
//
//        Node node9 = new ObjectNode("node120.dit.upm.es");
//        nodeId = node9.initPastry(node1, 120);
//        this.network.add(node9);
//
//        this.network.printOutNetwork();

    }

    @Test
    public void scenario_3() throws Exception {
        System.out.println("Test Scenario 3!\n");
        int nodeId = -1;

        PastryNode node1 = new ObjectNode("master53.dit.upm.es");
        node1.setBootstrapNode(null);
        nodeId = node1.initPastry(53);
        node1.run();
        this.network.add(node1);
        this.network.printOutNetwork();

        PastryNode node2 = new ObjectNode("node50.dit.upm.es");
        node2.setBootstrapNode(node1);
        nodeId = node2.initPastry(50);
        node2.run();
        this.network.add(node2);
        this.network.printOutNetwork();

        PastryNode node3 = new ObjectNode("node55.dit.upm.es");
        node3.setBootstrapNode(node1);
        nodeId = node3.initPastry(55);
        node3.run();
        this.network.add(node3);
        this.network.printOutNetwork();

        PastryNode node4 = new ObjectNode("node40.dit.upm.es");
        node4.setBootstrapNode(node1);
        nodeId = node4.initPastry(40);
        node4.run();
        this.network.add(node4);
        this.network.printOutNetwork();

        PastryNode node5 = new ObjectNode("node60.dit.upm.es");
        node5.setBootstrapNode(node1);
        nodeId = node5.initPastry(60);
        node5.run();
        this.network.add(node5);
        this.network.printOutNetwork();

        PastryNode node6 = new ObjectNode("node130.dit.upm.es");
        node6.setBootstrapNode(node1);
        nodeId = node6.initPastry(130);
        node6.run();
        this.network.add(node6);
        this.network.printOutNetwork();

        PastryNode node7 = new ObjectNode("node140.dit.upm.es");
        node7.setBootstrapNode(node1);
        nodeId = node7.initPastry(140);
        node7.run();
        this.network.add(node7);
        this.network.printOutNetwork();

        PastryNode node8 = new ObjectNode("node110.dit.upm.es");
        node8.setBootstrapNode(node1);
        nodeId = node8.initPastry(110);
        node8.run();
        this.network.add(node8);
        this.network.printOutNetwork();

        PastryNode node9 = new ObjectNode("node120.dit.upm.es");
        node9.setBootstrapNode(node1);
        nodeId = node9.initPastry(120);
        node9.run();
        this.network.add(node9);

        this.network.printOutNetwork();

        // NodeId 120 detects 130 departure
        //
        node9.onNodeLeave(130);

    }


}
