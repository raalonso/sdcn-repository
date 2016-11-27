package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.StateTable;

import java.util.*;
import java.util.logging.Logger;


public class ObjectNode implements Node<Object> {

    private static final Logger LOGGER = Logger.getLogger(ObjectNode.class.getName());

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private static final int b = 7;
    private static final int MAX_NODE_IDS = (int) Math.pow(2, b); // 2^b = 2^7 = 128 nodes
    private static final int l = 2;

    protected String name;
    protected int nodeId; // Unique numeric identifier of each node in the Pastry network

    // Leaf Set (L)
    // The leaf set L is the set of nodes with the | L | / 2 numerically
    // closest larger nodeIds, and the | L | / 2 nodes with numerically closest smaller nodeIds,
    // relative to the present node’s nodeId.
    protected int[] leafSet;
    protected Set smaller;
    protected Set larger;

    // Neighborhood Set (M)
    // The neighborhood set M contains the nodeIds and IP addresses of the nodes
    // that are closest (according the proximity metric) to the local node.
    protected Hashtable neighborhoodSet;

    // Routing Table (R): not used



    public ObjectNode(String name) {
        this.name = name;
        this.nodeId = -1;
    }

    public int initPastry(Node nearbyNode, int key) {

        if (key == 0) this.nodeId = createKey();
        else this.nodeId = key;

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId);

        this.neighborhoodSet = new Hashtable(l*2);

        leafSet = new int[l*2];
        this.smaller = new LinkedHashSet();
        this.larger = new LinkedHashSet();


        if (nearbyNode != null) {
            System.out.println("Pastry Node " + this.nodeId + ": Joining network...");

            if (nearbyNode == null) {
                System.out.println("Pastry Node " + this.nodeId + ": ERROR No known node to join Pastry network");
                return -1;
            }

            // Send a message to nearby node to join the network
            StateTable stateTable = nearbyNode.join(this);

            System.out.println("Pastry Node " + this.nodeId + ": Adding node " + nearbyNode.getNodeId() + " to local Routing Table");
            this.neighborhoodSet.put(nearbyNode.getNodeId(), nearbyNode);

            // Last node on the path from A to Z send their state tables to X
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN_RESPONSE (" + stateTable + ")" + ANSI_RESET);

            updateLeafSet(stateTable.getL());
            updateNeighborhoodSet(stateTable.getM());

            // Communicate neighbors that a new node has joined
            //
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Communicate neighbours that node " + this.nodeId + " joined Pastry network..." + ANSI_RESET);
            joined();
        }

        return this.nodeId;
    }


    public void joined() {
        System.out.println("Pastry Node " + this.nodeId + ": Sending BROADCAST_STATE to neighbors...");
        for (int i = 0; i < this.leafSet.length; i++) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": Neighbour Pastry node " + nodeId);
                Node node = (Node) this.neighborhoodSet.get(nodeId);

                StateTable stateTable = new StateTable();
                stateTable.setL(this.nodeId, this.leafSet);
                stateTable.setM(this.neighborhoodSet);

                node.broadcastState(this, stateTable);
            }
        }

    }

    @Override
    public void broadcastState(Node fromNode, StateTable stateTable) {
        int fromNodeId = fromNode.getNodeId();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received BROADCAST_STATE from " + fromNodeId + ANSI_RESET + " w/ State Table " + stateTable);

        updateLeafSet(stateTable.getL());

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding node " + fromNode.getNodeId() + " to local Routing Table..." + ANSI_RESET);
        this.neighborhoodSet.put(fromNode.getNodeId(), fromNode);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding nodes " + stateTable.getM().keySet() + " to local Routing Table..." + ANSI_RESET);
        this.neighborhoodSet.putAll(stateTable.getM());

    }

//    public void addNodeToLeafSet(int newNodeId) {
//        System.out.println("Pastry Node " + this.nodeId + ": Adding Node to Leaf Set (id=" + newNodeId + ") to Leaf Set " + Arrays.toString(this.leafSet) + "...");
//
//        int myNodeId = this.getNodeId();
//
//        if (this.smaller.isEmpty() && (newNodeId < myNodeId)) this.smaller.add(newNodeId);
//        else if (this.larger.isEmpty() && (newNodeId > myNodeId)) this.larger.add(newNodeId);
//        else if ((this.smaller.size() == 1) && (newNodeId < myNodeId)) this.smaller.add(newNodeId);
//        else if ((this.larger.size() == 1) && (newNodeId > myNodeId)) this.larger.add(newNodeId);
//        else {
//            Object first = this.larger.last();
//            Object last = this.larger.first();
//
//            if (newNodeId < (int) first) {
//                this.larger.remove(first);
//                this.larger.add(newNodeId);
//            } else if (((int) first < newNodeId) && (newNodeId < (int) last)) {
//                this.larger.remove(first);
//                this.larger.add(newNodeId);
//            }
//        }
//
//        // Create internal attribute for leaf seat
//        //
//        Object[] smaller_array = this.smaller.toArray();
//        Object[] larger_array = this.larger.toArray();
//
//        // Smaller
//        if (smaller_array.length >= 2) this.leafSet[0] = (int) smaller_array[1];
//        if (smaller_array.length >= 1) {
//            this.leafSet[1] = (int) smaller_array[0];
//        }
//        // Larger
//        if (larger_array.length == 2) this.leafSet[3] = (int) larger_array[1];
//        if (larger_array.length >= 1) this.leafSet[2] = (int) larger_array[0];
//
//        System.out.println("Pastry Node " + this.nodeId + ": LeafSet updated " + this);
//
//    }

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

        // LARGER
        this.larger.clear();
        if (LARGER.size() >= 1) this.larger.add(LARGER.get(0));
        if (LARGER.size() >= 2) this.larger.add(LARGER.get(1));

        // SMALLER
        this.smaller.clear();
        if (SMALLER.size() >= 1) this.smaller.add(SMALLER.get(0));
        if (SMALLER.size() >= 2) this.smaller.add(SMALLER.get(1));

        if (SMALLER.size() > 2) {
            if (this.larger.size() < 2) {
                if ((this.larger.size() == 0)) {
                    if (SMALLER.size() >= 3) this.larger.add(SMALLER.get(2));
                    if (SMALLER.size() >= 4) this.larger.add(SMALLER.get(3));
                }
                else if ((this.larger.size() == 1) && (SMALLER.size() >= 3)) this.larger.add(SMALLER.get(2));
            }
        }

        if (LARGER.size() > 2) {
            if (this.smaller.size() < 2) {
                if ((this.smaller.size() == 0)) {
                    if (LARGER.size() >= 3) this.smaller.add(LARGER.get(2));
                    if (LARGER.size() >= 4) this.smaller.add(LARGER.get(3));
                }
                else if ((this.smaller.size() == 1) && (LARGER.size() >= 3)) this.smaller.add(LARGER.get(2));
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": UPDATED Leaf Set SMALLER " + this.smaller);
        System.out.println("Pastry Node " + this.nodeId + ": UPDATED Leaf Set LARGER " + this.larger);

        // Create internal attribute for leaf seat
        //
        Object[] smaller_array = this.smaller.toArray();
        Object[] larger_array = this.larger.toArray();

        System.out.println("Pastry Node " + this.nodeId + ": SMALLER array " + smaller_array.toString());
        System.out.println("Pastry Node " + this.nodeId + ": LARGER array " + larger_array.toString());

        this.leafSet[0] = this.leafSet[1] = this.leafSet[2] = this.leafSet[3] = 0;

        // Smaller
        if (smaller_array.length == 0) {
            if (larger_array.length > 2) {
                if (larger_array.length >= 4) this.leafSet[0] = ((Integer) larger_array[0]).intValue();
                if (larger_array.length >= 3) this.leafSet[1] = ((Integer) larger_array[1]).intValue();
            }
        }
        if (smaller_array.length == 2) {
            this.leafSet[0] = ((Integer) smaller_array[1]).intValue();
            this.leafSet[1] = ((Integer) smaller_array[0]).intValue();
        }
        if (smaller_array.length == 1) {
            this.leafSet[0] = ((Integer) smaller_array[0]).intValue();
            this.leafSet[1] = 0;
        }

        // Larger
        if (larger_array.length == 0) {
            if (smaller_array.length > 2) {
                if (smaller_array.length >= 4) this.leafSet[3] = ((Integer) smaller_array[3]).intValue();
                if (smaller_array.length >= 3) this.leafSet[2] = ((Integer) smaller_array[2]).intValue();
            }
        }
        if (larger_array.length == 2) {
            this.leafSet[2] = ((Integer) larger_array[0]).intValue();
            this.leafSet[3] = ((Integer) larger_array[1]).intValue();
        }
        if (larger_array.length == 1) {
            this.leafSet[2] = ((Integer) larger_array[0]).intValue();
            this.leafSet[3] = 0;
        }

        System.out.println("Pastry Node " + this.nodeId + ": Leaf Set updated " + this);

    }

    @Override
    public void updateNeighborhoodSet(Hashtable neighborhoodSet) {
        this.neighborhoodSet.putAll(neighborhoodSet);
    }

    protected int distance(int fromNodeId, int toNodeId) {
        int result = 0;
        int d_forward = 0;
        int d_backward = 0;
        if (toNodeId >= fromNodeId) {
            d_forward = toNodeId - fromNodeId;
            d_backward = fromNodeId + (MAX_NODE_IDS - toNodeId);
        }
        else { // (toNodeId < fromNodeId)
            d_forward = (MAX_NODE_IDS - fromNodeId) + toNodeId;
            d_backward = fromNodeId - toNodeId;
        }

        System.out.println("Pastry Node " + this.nodeId + ": Distance from " + fromNodeId + " to " + toNodeId + " (-" + d_backward + "," + d_forward + ")");

        if (d_forward <= d_backward) result = d_forward;
        else result = d_backward * -1;

        return (result);
    }

    protected int[] calculateDistances(int[] nodes, int nodeId) {
        int[] result = new int[2];

        int[] leafDistances = new int[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            leafDistances[i] = 0;

            if (nodes[i] == 0) leafDistances[i] = 0;
            else {
//                leafDistances[i] = distance(nodes[i], nodeId);
                leafDistances[i] = distance(nodeId, nodes[i]);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": Distances of leafSet " + Arrays.toString(leafDistances));

        return leafDistances;
    }


    public boolean isInLeafSet(int nodeId, int[] leafDistances) {

        int smallerNodeId = -1;
        int largerNodeId = -1;

        // SMALLER
        if ((this.leafSet[0] == 0) && (this.leafSet[1]) == 0) smallerNodeId = 0;
        else if ((this.leafSet[0] == 0) && (this.leafSet[1]) != 0) smallerNodeId = this.leafSet[1];
        else if ((this.leafSet[0] != 0) && (this.leafSet[1]) != 0) smallerNodeId = this.leafSet[0];

        // LARGER
        if ((this.leafSet[3] == 0) && (this.leafSet[2]) == 0) largerNodeId = 0;
        else if ((this.leafSet[3] == 0) && (this.leafSet[2]) != 0) largerNodeId = this.leafSet[2];
        else if ((this.leafSet[3] != 0) && (this.leafSet[2]) != 0) largerNodeId = this.leafSet[3];

        System.out.println("Pastry Node " + this.nodeId + ": Smaller is " + smallerNodeId + " and larger is " + largerNodeId );

        if (largerNodeId == 0) largerNodeId = MAX_NODE_IDS;
        if ((smallerNodeId < nodeId) || (nodeId < largerNodeId)) {
            System.out.println("Pastry Node " + this.nodeId + ": NodeId " + nodeId + " is within range of our Leaf Set");
            return true;
        }
        else
            return false;
    }

    @Override
    public void route(String msg, int key) {
        // Any node A that receives a message M with destination address D routes the message by comparing D with its
        // own GUID A and with each of the GUIDs in its leaf set and forwarding M to the node amongst them that is
        // numerically closest to D.
    }

    protected int createKey() {
        int hash = Math.abs(this.name.hashCode() % MAX_NODE_IDS);

        return hash;
    }

    @Override
    public int getNodeId() {
        return this.nodeId;
    }

    @Override
    public Object get(int key) {
        return this.nodeId;
    }

    @Override
    public String toString() {
        return "ObjectNode {" +
                "name='" + name + '\'' +
                ", " + ANSI_RED + "nodeId=" + nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + "L=" + Arrays.toString(leafSet) + ANSI_RESET +
                ", M=" + this.neighborhoodSet.keySet() +
                '}';
    }

    @Override
    public StateTable join(Node node) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN request from " + node.getNodeId() + ANSI_RESET);

        // Maps nodeId to address of node (in local is address of object)
        //
        System.out.println("Pastry Node " + this.nodeId + ": Adding node " + node.getNodeId() + " to local Routing Table");
        this.neighborhoodSet.put(node.getNodeId(), node);

        // Create stateTable to send to other nodes
        //
        StateTable stateTable = new StateTable();
        stateTable.setL(this.nodeId, this.leafSet);

        int newNodeId = node.getNodeId();
        int myNodeId = this.getNodeId();

        int[] leafDistances = calculateDistances(this.leafSet, newNodeId);

        if (isInLeafSet(newNodeId, leafDistances)) { // leafDistances are not used
            // New nodeId is within range of our leaf set
            //
            int minDistance = MAX_NODE_IDS;
            int nodeIndex = -1;

            int d_fromLocalNode = distance(myNodeId, newNodeId);
            minDistance = d_fromLocalNode;

            int[] numbers = {1, 2, 0, 3};
            for (int i : numbers) {
                if ((leafDistances[i] != 0) && (Math.abs(leafDistances[i]) < Math.abs(minDistance))) {
                    nodeIndex = i;
                    minDistance = leafDistances[i];
                }
            }

            System.out.println("Pastry Node " + this.nodeId + ": Node index " + nodeIndex + " with min distance of " + minDistance);

            int[] result = new int[2];
            if (nodeIndex == -1) result[0] = myNodeId;
            else result[0] = leafSet[nodeIndex];
            result[1] = minDistance;

            int routeToNodeId = result[0];
            if (routeToNodeId == this.getNodeId()) {
                System.out.println("Pastry Node " + this.nodeId + ": Processing request in local node " + result[0] + " (with min distance of " + result[1] + ")");

                int[] newNode = {newNodeId};
                updateLeafSet(newNode);
//                addNodeToLeafSet(newNodeId);
            } else {
                Node fwdNode = (Node) this.neighborhoodSet.get(routeToNodeId);

                System.out.println("Pastry Node " + this.nodeId + ": Forward request to nodeId " + result[0] + " (with min distance of " + result[1] + ")");

                StateTable stateTable_X = fwdNode.join(node);

                return stateTable_X;
            }
        }
        else {
            // TODO: Add your code here...
            System.out.println("Pastry Node " + this.nodeId + ": ERROR ");
        }

        stateTable.setM(this.neighborhoodSet);

        return stateTable;

    }

    @Override
    public void leave() {

    }

    protected int[] getStateTable() {
        // Create stateTable to send to other nodes
        //
        int[] stateTable = new int[2 * l + 1];
        // Smaller
        stateTable[0] = this.leafSet[0];
        stateTable[1] = this.leafSet[1];
        // This node
        stateTable[2] = this.nodeId;
        // Larger
        stateTable[3] = this.leafSet[2];
        stateTable[4] = this.leafSet[3];

        return stateTable;
    }


    public static void main(String[] args) {
        System.out.println("LocalNode main!\n");
        int nodeId = -1;

        System.out.println("-1----");
        Node node1 = new ObjectNode("master53.dit.upm.es");
        nodeId = node1.initPastry(null, 53);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println("-----\n");

        System.out.println("-2----");
        Node node2 = new ObjectNode("node60.dit.upm.es");
        nodeId = node2.initPastry(node1, 60);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println("-----\n");

        System.out.println("-3----");
        Node node3 = new ObjectNode("node50.dit.upm.es");
        nodeId = node3.initPastry(node1, 50);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println(node3);
        System.out.println("-----\n");

        System.out.println("-4----");
        Node node4 = new ObjectNode("node65.dit.upm.es");
        nodeId = node4.initPastry(node1, 65);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println(node3);
        System.out.println(node4);
        System.out.println("-----\n");

        System.out.println("-5----");
        Node node5 = new ObjectNode("node120.dit.upm.es");
        nodeId = node5.initPastry(node1, 120);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println(node3);
        System.out.println(node4);
        System.out.println(node5);
        System.out.println("-----\n");

        System.out.println("-6----");
        Node node6 = new ObjectNode("node115.dit.upm.es");
        nodeId = node6.initPastry(node1, 115);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println(node3);
        System.out.println(node4);
        System.out.println(node5);
        System.out.println(node6);
        System.out.println("-----");


    }

}
