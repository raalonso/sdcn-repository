package es.dit.muirst.sdcn.dht;

import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.messaging.Message;

import java.util.*;
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

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public int getNodeId() {
        return this.nodeId;
    }

    public A get(int key) {
        return null;
    }


    @Override
    public void updateLeafSet(int[] leafs) {
        SortedSet stateTable = new TreeSet();

        for (int i = 0; i < leafs.length; i++) {
            if ((leafs[i] == this.nodeId) || (leafs[i] == 0)) {}
            else stateTable.add(leafs[i]);
        }

        for (int i = 0; i < this.leafSet.length; i++) {
            if ((this.leafSet[i] == this.nodeId) || (this.leafSet[i] == 0)) {}
            else stateTable.add(this.leafSet[i]);
        }

        System.out.println("Pastry Node " + this.nodeId + ": UPDATE Leaf Set TMP State Table " + stateTable);

        int[] nodes = new int[stateTable.size()];
        Iterator it = stateTable.iterator();
        int index = 0;
        while (it.hasNext()) {
            Integer element = (Integer) it.next();
            nodes[index++] = element.intValue();
        }

        System.out.println("Pastry Node " + this.nodeId + ": All nodes " + Arrays.toString(nodes));

        int[] distances = calculateDistances(nodes, this.nodeId);

        ArrayList<Integer> SMALLER = new ArrayList<Integer>();
        ArrayList<Integer> LARGER = new ArrayList<Integer>();

        int min;
        int mindex;
        for (int i = 0; i < nodes.length; i++) {
            min = MAX_NODE_IDS;
            mindex = -1;
            for (int j = 0; j < nodes.length; j++) {
                if (nodes[j] != -1) {
                    if (distances[j] > 0) {
                        if (distances[j] < min) {
                            min = distances[j];
                            mindex = j;
                        }
                    }
                }
            }
            if (mindex != -1) {
                System.out.println("Pastry Node " + this.nodeId + ": LARGER result {nodeId=" + nodes[mindex] + ";distance=" + distances[mindex] + "}");
                LARGER.add(nodes[mindex]);
                nodes[mindex] = -1;
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            min = MAX_NODE_IDS;
            mindex = -1;
            for (int j = 0; j < nodes.length; j++) {
                if (nodes[j] != -1) {
                    if (distances[j] < 0) {
                        if (Math.abs(distances[j]) < Math.abs(min)) {
                            min = distances[j];
                            mindex = j;
                        }
                    }
                }
            }
            if (mindex != -1) {
                System.out.println("Pastry Node " + this.nodeId + ": SMALLER result {nodeId=" + nodes[mindex] + ";distance=" + distances[mindex] + "}");
                SMALLER.add(nodes[mindex]);
                nodes[mindex] = -1;
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": Leaf Set SMALLER " + Arrays.toString(SMALLER.toArray()));
        System.out.println("Pastry Node " + this.nodeId + ": Leaf Set LARGER " + Arrays.toString(LARGER.toArray()));

        // Reset Leaf Set
        //
        this.leafSet[0] = this.leafSet[1] = this.leafSet[2] = this.leafSet[3] = 0;

        // Smaller
        if (SMALLER.size() >= 2) {
            int distance0 = Math.abs(distance(this.nodeId,  SMALLER.get(0)));
            int distance1 = Math.abs(distance(this.nodeId, SMALLER.get(1)));

            if (distance0 < distance1) {
                this.leafSet[0] = SMALLER.get(1);
                this.leafSet[1] = SMALLER.get(0);
            }
            else {
                this.leafSet[0] = SMALLER.get(0);
                this.leafSet[1] = SMALLER.get(1);
            }
        }
        if (SMALLER.size() == 1) {
            this.leafSet[0] = 0;
            this.leafSet[1] = SMALLER.get(0);
        }

        // Larger
        if (LARGER.size() >= 2) {
            int distance0 = Math.abs(distance(this.nodeId, LARGER.get(0)));
            int distance1 = Math.abs(distance(this.nodeId, LARGER.get(1)));

            if (distance0 < distance1) {
                this.leafSet[2] = LARGER.get(0);
                this.leafSet[3] = LARGER.get(1);
            }
            else {
                this.leafSet[2] = LARGER.get(1);
                this.leafSet[3] = LARGER.get(0);
            }
        }
        if (LARGER.size() == 1) {
            this.leafSet[2] = LARGER.get(0);
            this.leafSet[3] = 0;
        }


        if (SMALLER.size() == 1) {
            if (LARGER.size() >= 3) {
                this.leafSet[0] = LARGER.get(LARGER.size()-1);
            }
        }
        if (SMALLER.size() == 0) {
            if (LARGER.size() == 2) {
                this.leafSet[1] = this.leafSet[3];
                this.leafSet[3] = 0;
            }
            if (LARGER.size() == 3) {
                this.leafSet[1] = LARGER.get(2);
            }
            if (LARGER.size() >= 4) {
                this.leafSet[0] = LARGER.get(LARGER.size()-2);
                this.leafSet[1] = LARGER.get(LARGER.size()-1);
            }
        }

        if (LARGER.size() == 1) {
            if (SMALLER.size() >= 3) {
                this.leafSet[3] = SMALLER.get(SMALLER.size()-1);
            }
        }
        if (LARGER.size() == 0) {
            if (SMALLER.size() == 2) {
                this.leafSet[2] = this.leafSet[0];
                this.leafSet[0] = 0;
            }
            if (SMALLER.size() == 3) {
                this.leafSet[2] = SMALLER.get(2);
            }
            if (SMALLER.size() >= 4) {
                this.leafSet[2] = SMALLER.get(SMALLER.size()-1);
                this.leafSet[3] = SMALLER.get(SMALLER.size()-2);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Leaf Set updated " + ANSI_RESET + this);

    }

    @Override
    public void updateNeighborhoodSet(Hashtable neighborhoodSet) {
        System.out.println("Pastry Node " + this.nodeId + ": " + "Adding nodes " + neighborhoodSet.keySet() + " to Neighborhood Set...");

        this.neighborhoodSet.putAll(neighborhoodSet);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Neighborhood Set updated " + ANSI_RESET + this);
    }

    /////

    protected int createKey() {
        int hash = Math.abs(this.name.hashCode() % MAX_NODE_IDS);

        return hash;
    }

    protected int distance(int fromNodeId, int toNodeId) {
        int result = 0;
        int d_forward = 0;
        int d_backward = 0;
        if (toNodeId >= fromNodeId) {
            d_forward = toNodeId - fromNodeId;
            d_backward = fromNodeId + (MAX_NODE_IDS - toNodeId);
        }
        else {
            // (toNodeId < fromNodeId)
            d_forward = (MAX_NODE_IDS - fromNodeId) + toNodeId;
            d_backward = fromNodeId - toNodeId;
        }

        System.out.println("Pastry Node " + this.nodeId + ": f(distance) from " + fromNodeId + " to " + toNodeId + " (bw=" + d_backward + ", fw=" + d_forward + ")");

        if (d_forward <= d_backward) result = d_forward;
        else result = d_backward * -1;

        return result;
    }

    protected int[] calculateDistances(int[] nodes, int nodeId) {
        int[] result = new int[2];

        int[] leafDistances = new int[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            leafDistances[i] = 0;

            if (nodes[i] == 0) leafDistances[i] = 0;
            else {
                leafDistances[i] = distance(nodeId, nodes[i]);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": Nodes in Leaf Set     " + Arrays.toString(this.leafSet));
        System.out.println("Pastry Node " + this.nodeId + ": Distances of Leaf Set " + Arrays.toString(leafDistances));

        return leafDistances;
    }

    public boolean isInLeafSet(int newNodeId, int routeToNodeId) {
        if (routeToNodeId == this.getNodeId()) {
            System.out.println("Pastry Node " + this.nodeId + ": NodeId " + newNodeId + " is within range of local node Leaf Set");
            return true;
        }
        else {
            return false;
        }
    }

    public String toString() {
        return "Pastry Node {" +
                "name='" + name + '\'' +
                ", " + ANSI_RED + "nodeId=" + this.nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + "L=" + Arrays.toString(this.leafSet) + ANSI_RESET +
                ", M=" + this.neighborhoodSet +
                '}';
    }

}
