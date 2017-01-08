package es.dit.muirst.sdcn.dht;

import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.messaging.Message;

import java.util.Hashtable;
import java.util.logging.Logger;


public abstract class PastryNode<A> implements Node {

    private static final Logger LOGGER = Logger.getLogger(PastryNode.class.getName());

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    protected static final int b = 8;
    protected static final int MAX_NODE_IDS = (int) Math.pow(2, b); // 2^b = 2^8 = 256 nodes
    protected static final int l = 2;

    protected String name; // Name of the node
    protected int nodeId; // Unique numeric identifier of each node in the Pastry network

    // Leaf Set (L)
    // The leaf set L is the set of nodes with the | L | / 2 numerically
    // closest larger nodeIds, and the | L | / 2 nodes with numerically closest smaller nodeIds,
    // relative to the present node’s nodeId.
    protected int[] leafSet;

    // Neighborhood Set (M)
    // The neighborhood set M contains the nodeIds and IP addresses of the nodes
    // that are closest (according the proximity metric) to the local node.
    protected Hashtable neighborhoodSet;

    // Routing Table (R): not used

    //
    protected Hashtable localData;

    //
    protected A bootstrapNode;



    public PastryNode(String name) {
        this.name = name;
        this.nodeId = -1;

        this.neighborhoodSet = new Hashtable(l*2);
        leafSet = new int[l*2];

        this.localData = new Hashtable();
    }

    public void setBootstrapNode(A address) {
        this.bootstrapNode = address;
    }

    @Override
    public int initPastry(int key) throws Exception {
        return 0;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public int getNodeId() {
        return 0;
    }

    public A get(int key) {
        return null;
    }

    @Override
    public void route(Message msg, int key) {

    }

    @Override
    public StateTable join(Node node) {
        return null;
    }

    @Override
    public void leave(Node fromNode, int nodeId, StateTable stateTable) {

    }

    @Override
    public void broadcastState(Node fromNode, StateTable stateTable) {

    }

    @Override
    public void onNodeLeave(int nodeId) {

    }

    @Override
    public void updateLeafSet(int[] leafs) {

    }

    @Override
    public void updateNeighborhoodSet(Hashtable neighborhoodSet) {

    }

    /////

    protected int createKey() {
        int hash = Math.abs(this.name.hashCode() % MAX_NODE_IDS);

        return hash;
    }


}
