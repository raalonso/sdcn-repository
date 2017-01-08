package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.PastryNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class DHTDataTest {

    OverlayNetwork network;

    @Before
    public void setUp() throws Exception {
        this.network = new OverlayNetwork();

        System.out.println("Setup Scenario!\n");
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

    }

    @After
    public void tearDown() throws Exception {
        this.network = null;
    }

    @Test
    public void data_scenario_1() throws Exception {
        ObjectNode bootstrap_node = (ObjectNode) this.network.nodes.get(2);

        System.out.println("Pastry Bootstrap Node " + bootstrap_node.getNodeId());

        bootstrap_node.putData(49, "TOKEN_49|Hello World!");
        bootstrap_node.putData(52, "TOKEN_52|Radiohead");
        bootstrap_node.putData(110, "TOKEN_110|Pink Floyd");
        bootstrap_node.putData(1, "TOKEN_1|Anyone can play guitar");
        bootstrap_node.putData(250, "TOKEN_250|This is why we play");

        this.network.printOutDHT();

        ObjectNode node = (ObjectNode) this.network.nodes.get(4);

        System.out.println("Pastry Node " + node + "\n");

        String data;
        int key;

        key = 110;
        data = node.getData(key);
        System.out.println(">> Data for key " + key + ": " + data + "\n");

        key = 250;
        data = node.getData(key);
        System.out.println(">> Data for key " + key + ": " + data + "\n");

        key = 49;
        data = node.getData(key);
        System.out.println(">> Data for key " + key + ": " + data + "\n");


        key = 250;
        node.removeData(key);

        this.network.printOutDHT();

    }
}
