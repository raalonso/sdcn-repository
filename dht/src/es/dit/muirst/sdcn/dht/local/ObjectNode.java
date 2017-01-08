package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.interfaces.DHT;
import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.messaging.Message;
import es.dit.muirst.sdcn.dht.messaging.PutDataRequest;
import es.dit.muirst.sdcn.dht.messaging.RemoveDataRequest;

import java.util.*;
import java.util.logging.Logger;


public class ObjectNode implements Node<Object>, DHT<String> {

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

    private static final int b = 8;
    private static final int MAX_NODE_IDS = (int) Math.pow(2, b); // 2^b = 2^8 = 256 nodes
    private static final int l = 2;

    protected String name;
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



    public ObjectNode(String name) {
        this.name = name;
        this.nodeId = -1;

        this.localData = new Hashtable();
    }

    public int initPastry(Node bootstrapNode, int key) {

        if (key == 0) this.nodeId = createKey();
        else this.nodeId = key;

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId);

        this.neighborhoodSet = new Hashtable(l*2);

        leafSet = new int[l*2];

        if (bootstrapNode != null) {
            System.out.println("Pastry Node " + this.nodeId + ": Joining network...");

            if (bootstrapNode == null) {
                System.out.println("Pastry Node " + this.nodeId + ": ERROR No known node to join Pastry network");
                return -1;
            }

            // Send a message to nearby node to join the network
            //
            StateTable stateTable = bootstrapNode.join(this);

            System.out.println("Pastry Node " + this.nodeId + ": Adding node " + bootstrapNode.getNodeId() + " to local Routing Table");
            this.neighborhoodSet.put(bootstrapNode.getNodeId(), bootstrapNode);

            // Last node on the path from A to Z sends their state tables to X
            //
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
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                Node node = (Node) this.neighborhoodSet.get(nodeId);

                StateTable stateTable = new StateTable();
                stateTable.setL(this.nodeId, this.leafSet);
                stateTable.setM(this.neighborhoodSet);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending BROADCAST_STATE to neighbor " + nodeId + "..." + ANSI_RESET);
                node.broadcastState(this, stateTable);
            }
        }

    }

    public void broadcastData(int key, String data, boolean fwEnabled) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Sending BROADCAST_DATA to Leaf Set..." + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3}; // order to send BROADCAST_DATA

        for (int i : numbers) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                DHT node = (DHT) this.neighborhoodSet.get(nodeId);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Deliver PUT_DATA_REQUEST to nodeId " + node.getNodeId() + ANSI_RESET);
                PutDataRequest request = new PutDataRequest(key, data);

                if ((fwEnabled) && ((i == 0) || (i == 3))) {
                    request.setFw_flag(true);
                }

                node.putDataRequest(request);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Ended BROADCAST_DATA to Leaf Set..." + ANSI_RESET);
    }

    public void broadcastRemoveData(int key) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Sending BROADCAST_REMOVE_DATA to Leaf Set..." + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3}; // order to send BROADCAST_REMOVE_DATA

        for (int i : numbers) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                DHT node = (DHT) this.neighborhoodSet.get(nodeId);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Deliver REMOVE_DATA_REQUEST to nodeId " + node.getNodeId() + ANSI_RESET);
                RemoveDataRequest request = new RemoveDataRequest(key);
                node.removeDataRequest(request);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Ended BROADCAST_DATA to Leaf Set..." + ANSI_RESET);
    }

    @Override
    public void broadcastState(Node fromNode, StateTable stateTable) {
        // Actually the method SHOULD be call onBroadcastState

        int fromNodeId = fromNode.getNodeId();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received BROADCAST_STATE from " + fromNodeId + ANSI_RESET + " with " + stateTable);

        System.out.println("Pastry Node " + this.nodeId + ": " + "Updating Leaf Set...");
        updateLeafSet(stateTable.getL());

        System.out.println("Pastry Node " + this.nodeId + ": " + "Updating Neighborhood Set...");
        updateNeighborhoodSet(stateTable.getM());

    }

    @Override
    public void onNodeLeave(int departureNodeId) {
        System.out.println("Pastry Node " + this.nodeId + ": Sending LEAVE to neighbors...");

        for (int i = 0; i < this.leafSet.length; i++) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                Node node = (Node) this.neighborhoodSet.get(nodeId);

                StateTable stateTable = new StateTable();
                stateTable.setL(this.nodeId, this.leafSet);
                stateTable.setM(this.neighborhoodSet);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending LEAVE to neighbor " + nodeId + "..." + ANSI_RESET);
                node.leave(this, nodeId, stateTable);
            }
        }

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

    @Override
    public void route(Message msg, int key) {
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
                ", " + ANSI_RED + "nodeId=" + this.nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + "L=" + Arrays.toString(this.leafSet) + ANSI_RESET +
                ", M=" + this.neighborhoodSet.keySet() +
                '}';
    }

    public String printOutLocalDHT() {
        return "LocalDHT {" +
                ANSI_RED + "nodeId=" + this.nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + " DATA=" + this.localData.keySet() + ANSI_RESET +
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

        // result is a tuple with nodeId, distance
        int[] result = new int[2];
        if (nodeIndex == -1) result[0] = myNodeId;
        else result[0] = leafSet[nodeIndex];
        result[1] = minDistance;

        int routeToNodeId = result[0];

        if (isInLeafSet(newNodeId, routeToNodeId)) {
            System.out.println("Pastry Node " + this.nodeId + ": Processing request in local node " + result[0] + " (with min distance of " + result[1] + ")");

            int[] newNode = {newNodeId};
            updateLeafSet(newNode);

        } else {
            Node fwdNode = (Node) this.neighborhoodSet.get(routeToNodeId);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Route JOIN request to nodeId " + result[0] + " (with min distance of " + result[1] + ")" + ANSI_RESET);

            StateTable stateTable_X = fwdNode.join(node);

            return stateTable_X;

        }

        stateTable.setM(this.neighborhoodSet);

        return stateTable;

    }

    @Override
    public void leave(Node fromNode, int nodeId, StateTable stateTable) {
        // TODO: Add your code here...
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

    public boolean isDataInLeafSet(int key, int routeToNodeId) {
//        if ((routeToNodeId == this.getNodeId()) || (routeToNodeId == this.leafSet[1]) || (routeToNodeId == this.leafSet[2])) {
        if (routeToNodeId == this.getNodeId()) {
            System.out.println("Pastry Node " + this.nodeId + ": DATA key " + key + " is within range of local node Leaf Set");
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void putData(int key, String data) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received PUT_DATA '" + data + "' with key " + key + ANSI_RESET);

        int myNodeId = this.getNodeId();

        // Calculate distance with local nodeId and received key for the data
        //
        int[] leafDistances = calculateDistances(this.leafSet, key);
        int d_fromLocalNode = distance(key, myNodeId);

        // Data key is within range of our leaf set
        //
        int minDistance = d_fromLocalNode;
        int nodeIndex = -1;

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Distance from local node to key " + d_fromLocalNode + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3};
        for (int i : numbers) {
            if (leafDistances[i] == 0) {
                nodeIndex = i;
                minDistance = 0;
            }
            else if (Math.abs(leafDistances[i]) < Math.abs(minDistance)) {
                nodeIndex = i;
                minDistance = leafDistances[i];
            }
        }

//        System.out.println("Pastry Node " + this.nodeId + ": Node {index=" + nodeIndex + ";nodeId=" + leafSet[nodeIndex] + "} with min distance of " + minDistance);

        ////
        // The variable result is a tuple with nodeId, distance
        //
        int[] result = new int[2];

        if (nodeIndex == -1) result[0] = myNodeId;
        else result[0] = leafSet[nodeIndex];

        result[1] = minDistance;
        //
        ////

        int routeToNodeId = result[0];

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Route to nodeId " + routeToNodeId + ANSI_RESET);

        if (isDataInLeafSet(key, routeToNodeId)) {
            if (!this.localData.containsKey(key)) {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key inside local Leaf Set... stored data in Local Hash Table " + ANSI_RESET);

                // Stored data in local Hash Table
                //
                this.localData.put(key, data);
            }
            else {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key ALREADY stored in Local Hash Table " + ANSI_RESET);
            }

            // Forward data to all nodes in the leaf set
            //
            broadcastData(key, data, true);

        } else {
            DHT fwdNode = (DHT) this.neighborhoodSet.get(routeToNodeId);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Route PUT_DATA request to nodeId " + routeToNodeId + ANSI_RESET);

            fwdNode.putData(key, data);

            return;

        }

    }

    public int greaterBwDistances(int[] bw_distances) {
        int result = 0;
        for (int i : bw_distances) {
            if (i < 0) {
                if (Math.abs(i) > result) result = Math.abs(i);
            }
        }
        return result;
    }

    public int greaterFwDistances(int[] fw_distances) {
        int result = 0;
        for (int i : fw_distances) {
            if (i > 0) {
                if (Math.abs(i) > result) result = Math.abs(i);
            }
        }
        return result;
    }

    @Override
    public void putDataRequest(PutDataRequest request) {
        // actually should be called onPutDataRequest
        //

        // Request parameters, key and data
        //
        int key = request.getKey();
        String data = request.getData();

        System.out.println("\nPastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received PUT_DATA_REQUEST " + data + " with key " + key + ANSI_RESET);

        int myNodeId = this.getNodeId();

        // Calculate distance with local nodeId and received key for the data
        //
        int[] leafDistances = calculateDistances(this.leafSet, key);
        int d_fromLocalNode = distance(key, myNodeId); // (from, to)

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Distance from local node to key " + d_fromLocalNode + ANSI_RESET);

        // Key inside Leaf Set
        //
//        boolean isInsideLeafSet = false;
//        int distance = 0;
//        if ((myNodeId < this.leafSet[2]) && (myNodeId < this.leafSet[3]) && (this.leafSet[2] < this.leafSet[3]) && (key > this.leafSet[2]) && (key < this.leafSet[3])) {
//            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + "isInsideLeafSet = true CASE 1" + ANSI_RESET);
//            isInsideLeafSet = true;
//        }
//        else if ((this.leafSet[0] < this.leafSet[1]) && (this.leafSet[1] < this.leafSet[2]) && (this.leafSet[2] > this.leafSet[3])) {
//            // Example: [110 120 (130) 140 40]
//            if ((key > this.leafSet[0]) && (key < this.leafSet[2])) {
//                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + "isInsideLeafSet = true CASE 2" + ANSI_RESET);
//                isInsideLeafSet = true;
//            }
//            else if ((key > this.leafSet[2]) && (key < this.leafSet[3])) {
//                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + "isInsideLeafSet = true CASE 3" + ANSI_RESET);
//                isInsideLeafSet = true;
//            }
//        }
//        else if ((this.leafSet[0] < this.leafSet[1]) && (this.leafSet[1] < this.leafSet[2]) && (this.leafSet[2] < this.leafSet[3])) {
//            if ((key > this.leafSet[0]) && (key < this.leafSet[3])) {
//                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + "isInsideLeafSet = true CASE 4" + ANSI_RESET);
//                isInsideLeafSet = true;
//            }
//        }
//        else {
//            if (d_fromLocalNode >= 0) {
//                distance = greaterFwDistances(leafDistances);
//            } else {
//                // d_fromLocalNode < 0
//                distance = greaterBwDistances(leafDistances);
//            }
//
//            if (Math.abs(d_fromLocalNode) < Math.abs(distance)) {
//                isInsideLeafSet = true;
//            }
//        }

        boolean isInsideLeafSet = true;
        if (isInsideLeafSet) {
            if (!this.localData.containsKey(key)) {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key inside local Leaf Set... stored data in Local Hash Table " + ANSI_RESET);

                // Stored data in local Hash Table
                //
                this.localData.put(key, data);
            }
            else {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key ALREADY stored in Local Hash Table " + ANSI_RESET);
            }
        }

//        if (request.isFw_flag()) {
//            // Forward data to all nodes in the leaf set
//            //
//            broadcastData(key, data, false);
//        }

    }

    @Override
    public void removeDataRequest(RemoveDataRequest request) {
        // actually should be called onRemoveDataRequest
        //

        // Request parameters, key and data
        //
        int key = request.getKey();

        System.out.println("\nPastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received REMOVE_DATA_REQUEST with key " + key + ANSI_RESET);

        int myNodeId = this.getNodeId();

        // Calculate distance with local nodeId and received key for the data
        //
        int[] leafDistances = calculateDistances(this.leafSet, key);
        int d_fromLocalNode = distance(key, myNodeId); // (from, to)

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Distance from local node to key " + d_fromLocalNode + ANSI_RESET);

        // Key inside Leaf Set
        //
        boolean isInsideLeafSet = true;
        if (isInsideLeafSet) {
            if (this.localData.containsKey(key)) {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key in local Hash Table... remove data in Local Hash Table" + ANSI_RESET);

                // Remove data in local Hash Table
                //
                this.localData.remove(key);
            }
            else {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key NOT stored in Local Hash Table " + ANSI_RESET);
            }
        }

    }

    @Override
    public String getData(int key) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received GET_DATA with key " + key + ANSI_RESET);

        if (this.localData.containsKey(key)) {
            System.out.println("Pastry Node " + this.nodeId + ": DATA key " + key + " in local Hash Table");

            return (String) this.localData.get(key);
        }

        int myNodeId = this.getNodeId();

        // Calculate distance with local nodeId and received key for the data
        //
        int[] leafDistances = calculateDistances(this.leafSet, key);
        int d_fromLocalNode = distance(key, myNodeId);

        // Data key is within range of our leaf set
        //
        int minDistance = d_fromLocalNode;
        int nodeIndex = -1;

        int[] numbers = {1, 2, 0, 3};
        for (int i : numbers) {
            if (Math.abs(leafDistances[i]) < Math.abs(minDistance)) {
                nodeIndex = i;
                minDistance = leafDistances[i];
            }
        }

        ////
        // The variable result is a tuple with nodeId, distance
        //
        int[] result = new int[2];

        if (nodeIndex == -1) result[0] = myNodeId;
        else result[0] = leafSet[nodeIndex];

        result[1] = minDistance;
        //
        ////

        int routeToNodeId = result[0];

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Route GET_DATA to nodeId " + routeToNodeId + ANSI_RESET);

        DHT fwdNode = (DHT) this.neighborhoodSet.get(routeToNodeId);

        String data = (String) fwdNode.getData(key);

        return data;

    }

    @Override
    public void removeData(int key) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received REMOVE_DATA with key " + key + ANSI_RESET);

        int myNodeId = this.getNodeId();

        // Calculate distance with local nodeId and received key for the data
        //
        int[] leafDistances = calculateDistances(this.leafSet, key);
        int d_fromLocalNode = distance(key, myNodeId);

        // Data key is within range of our leaf set
        //
        int minDistance = d_fromLocalNode;
        int nodeIndex = -1;

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Distance from local node to key " + d_fromLocalNode + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3};
        for (int i : numbers) {
            if (Math.abs(leafDistances[i]) < Math.abs(minDistance)) {
                nodeIndex = i;
                minDistance = leafDistances[i];
            }
        }

        ////
        // The variable result is a tuple with nodeId, distance
        //
        int[] result = new int[2];

        if (nodeIndex == -1) result[0] = myNodeId;
        else result[0] = leafSet[nodeIndex];

        result[1] = minDistance;
        //
        ////

        int routeToNodeId = result[0];

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Route to nodeId " + routeToNodeId + ANSI_RESET);

        if (isDataInLeafSet(key, routeToNodeId)) {
            if (this.localData.containsKey(key)) {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key in local Hash Table... remove data in Local Hash Table" + ANSI_RESET);

                // Remove data in local Hash Table
                //
                this.localData.remove(key);
            }
            else {
                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_PURPLE + "Key NOT stored in Local Hash Table " + ANSI_RESET);
            }

            // Forward remove data to all nodes in the leaf set
            //
            broadcastRemoveData(key);

        } else {
            DHT fwdNode = (DHT) this.neighborhoodSet.get(routeToNodeId);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Route REMOVE_DATA_REQUEST to nodeId " + routeToNodeId + ANSI_RESET);

            fwdNode.removeData(key);

        }

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

        System.out.println("-7----");
        Node node7 = new ObjectNode("node127.dit.upm.es");
        nodeId = node7.initPastry(node1, 127);

        System.out.println("-----");
        System.out.println(node1);
        System.out.println(node2);
        System.out.println(node3);
        System.out.println(node4);
        System.out.println(node5);
        System.out.println(node6);
        System.out.println(node7);
        System.out.println("-----");


    }

}
