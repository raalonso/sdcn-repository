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
    private static final int MAX_NODE_IDS = (int) Math.pow(2, b); // 2^b = 2^7 = 128
    private static final int l = 2;

    protected String name;
    protected int nodeId;

    // Routing table:
    Hashtable routingTable;

    // Neighborhood set: not normally used in the routing process

    // Leaf set
    protected int[] leafSet;
    //    protected SortedSet smaller;
//    protected SortedSet larger;
    protected Set smaller;
    protected Set larger;


    public ObjectNode(String name) {
        this.name = name;
        this.nodeId = -1;
    }

    public int initPastry(Node nearbyNode, int key) {

        if (key == 0) this.nodeId = createKey();
        else this.nodeId = key;

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId);

        this.routingTable = new Hashtable(l*2);

        leafSet = new int[l*2];
//        this.smaller = new TreeSet();
//        this.larger = new TreeSet();
        this.smaller = new LinkedHashSet();
        this.larger = new LinkedHashSet();



        if (nearbyNode != null) {
            System.out.println("Pastry Node " + this.nodeId + ": Joining network...");

            // Send a message to nearby node to join the network
            StateTable stateTable = nearbyNode.join(this);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding node " + nearbyNode.getKey() + " to local Routing Table" + ANSI_RESET);
            this.routingTable.put(nearbyNode.getKey(), nearbyNode);

            // Last node on the path from A to Z send their state tables to X
            System.out.println("Pastry Node " + this.nodeId + ": Received STATE_TABLE " + Arrays.toString(stateTable.getL()));

            updateLeafSet(stateTable.getL());
            updateRoutingTable(stateTable.getR());

            // Communicate neighbors that a new node has joined
            //
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Communicate neighbours that node " + this.nodeId + " just JOINED..." + ANSI_RESET);
            joined();
        }

        return this.nodeId;
    }

    public void joined() {
        System.out.println("Pastry Node " + this.nodeId + ": Sending JOINED to neighbors...");
        for (int i = 0; i < this.leafSet.length; i++) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": Neighbour Pastry node " + nodeId);
                Node node = (Node) this.routingTable.get(nodeId);

                StateTable stateTable = new StateTable();
                stateTable.setL(this.nodeId, this.leafSet);
                stateTable.setR(this.routingTable);

                node.updateState(this, stateTable);
            }
        }

    }

    @Override
    public void updateState(Node fromNode, StateTable stateTable) {
        int fromNodeId = fromNode.getKey();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received UPDATE_STATE request from " + fromNodeId + ANSI_RESET + " w/ State Table " + stateTable);

        updateLeafSet(stateTable.getL());

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding node " + fromNode.getKey() + " to local Routing Table..." + ANSI_RESET);
        this.routingTable.put(fromNode.getKey(), fromNode);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding nodes " + stateTable.getR().keySet() + " to local Routing Table..." + ANSI_RESET);
        this.routingTable.putAll(stateTable.getR());

    }

//    public void addNodeToLeafSet(int newNodeId) {
//        System.out.println("Pastry Node " + this.nodeId + ": Adding Node to Leaf Set (id=" + newNodeId + ") to Leaf Set " + Arrays.toString(this.leafSet) + "...");
//
//        int myNodeId = this.getKey();
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
    public void updateRoutingTable(Hashtable routingTable) {
        this.routingTable.putAll(routingTable);
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
        if ((leafSet[0] == 0) && (leafSet[1]) == 0) smallerNodeId = 0;
        else if ((leafSet[0] == 0) && (leafSet[1]) != 0) smallerNodeId = leafSet[1];
        else if ((leafSet[0] != 0) && (leafSet[1]) != 0) smallerNodeId = leafSet[0];

        // LARGER
        if ((leafSet[3] == 0) && (leafSet[2]) == 0) largerNodeId = 0;
        else if ((leafSet[3] == 0) && (leafSet[2]) != 0) largerNodeId = leafSet[2];
        else if ((leafSet[3] != 0) && (leafSet[2]) != 0) largerNodeId = leafSet[3];

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
    public int getKey() {
        return this.nodeId;
    }

    @Override
    public Object get(int key) {
        return this.nodeId;
    }

    @Override
    public String toString() {
        return "LocalNode{" +
                "name='" + name + '\'' +
                ", " + ANSI_RED + "nodeId=" + nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + "leafSet=" + Arrays.toString(leafSet) + ANSI_RESET +
                ", routingTable=" + this.routingTable.keySet() +
                '}';
    }

    @Override
    public StateTable join(Node node) {
        System.out.println("Pastry Node " + this.nodeId + ": Received JOIN request from " + node.getKey());

        // Maps nodeId to address of node (in local is address of object)
        //
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Adding node " + node.getKey() + " to local Routing Table" + ANSI_RESET);
        this.routingTable.put(node.getKey(), node);

        // Create stateTable to send to other nodes
        //
        StateTable stateTable = new StateTable();
        stateTable.setL(this.nodeId, this.leafSet);

        int newNodeId = node.getKey();
        int myNodeId = this.getKey();

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
            if (routeToNodeId == this.getKey()) {
                System.out.println("Pastry Node " + this.nodeId + ": Processing request in local node " + result[0] + " (with min distance of " + result[1] + ")");

                int[] newNode = {newNodeId};
                updateLeafSet(newNode);
//                addNodeToLeafSet(newNodeId);
            } else {
                Node fwdNode = (Node) this.routingTable.get(routeToNodeId);

                System.out.println("Pastry Node " + this.nodeId + ": Forward request to nodeId " + result[0] + " (with min distance of " + result[1] + ")");

                StateTable stateTable_X = fwdNode.join(node);

                return stateTable_X;
            }
        }
        else {
            // TODO: Add your code here...
            System.out.println("Pastry Node " + this.nodeId + ": ERROR ");
        }

        stateTable.setR(this.routingTable);

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
