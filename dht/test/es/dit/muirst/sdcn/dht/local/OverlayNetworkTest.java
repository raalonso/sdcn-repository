package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.interfaces.Node;
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

        Node node1 = new ObjectNode("master53.dit.upm.es");
        nodeId = node1.initPastry(null, 53);
        this.network.add(node1);

        Node node2 = new ObjectNode("node60.dit.upm.es");
        nodeId = node2.initPastry(node1, 60);

        Node node3 = new ObjectNode("node50.dit.upm.es");
        nodeId = node3.initPastry(node1, 50);

        Node node4 = new ObjectNode("node65.dit.upm.es");
        nodeId = node4.initPastry(node1, 65);

        Node node5 = new ObjectNode("node120.dit.upm.es");
        nodeId = node5.initPastry(node1, 120);

        Node node6 = new ObjectNode("node115.dit.upm.es");
        nodeId = node6.initPastry(node1, 115);

        Node node7 = new ObjectNode("node127.dit.upm.es");
        nodeId = node7.initPastry(node1, 127);

    }

    @Test
    public void scenario_2() throws Exception {
        System.out.println("Test Scenario 2!\n");
        int nodeId = -1;

        Node node1 = new ObjectNode("master53.dit.upm.es");
        nodeId = node1.initPastry(null, 53);
        this.network.add(node1);
        this.network.printOutNetwork();

        Node node2 = new ObjectNode("node50.dit.upm.es");
        nodeId = node2.initPastry(node1, 50);
        this.network.add(node2);
        this.network.printOutNetwork();

        Node node3 = new ObjectNode("node55.dit.upm.es");
        nodeId = node3.initPastry(node1, 55);
        this.network.add(node3);
        this.network.printOutNetwork();

        Node node4 = new ObjectNode("node40.dit.upm.es");
        nodeId = node4.initPastry(node1, 40);
        this.network.add(node4);
        this.network.printOutNetwork();

        Node node5 = new ObjectNode("node60.dit.upm.es");
        nodeId = node5.initPastry(node1, 60);
        this.network.add(node5);
        this.network.printOutNetwork();

        Node node6 = new ObjectNode("node130.dit.upm.es");
        nodeId = node6.initPastry(node1, 130);
        this.network.add(node6);
        this.network.printOutNetwork();

        Node node7 = new ObjectNode("node140.dit.upm.es");
        nodeId = node7.initPastry(node1, 140);
        this.network.add(node7);
        this.network.printOutNetwork();

        Node node8 = new ObjectNode("node110.dit.upm.es");
        nodeId = node8.initPastry(node1, 110);
        this.network.add(node8);
        this.network.printOutNetwork();

        Node node9 = new ObjectNode("node120.dit.upm.es");
        nodeId = node9.initPastry(node1, 120);
        this.network.add(node9);

        this.network.printOutNetwork();

    }



}
