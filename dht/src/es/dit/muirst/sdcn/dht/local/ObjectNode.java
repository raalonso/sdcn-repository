package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.PastryNode;
import es.dit.muirst.sdcn.dht.interfaces.DHT;
import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.messaging.GetDataRequest;
import es.dit.muirst.sdcn.dht.messaging.PastryMessage;
import es.dit.muirst.sdcn.dht.messaging.PutDataRequest;
import es.dit.muirst.sdcn.dht.messaging.RemoveDataRequest;

import java.util.*;
import java.util.logging.Logger;


public class ObjectNode extends PastryNode<Object> implements DHT<String> {

    private static final Logger LOGGER = Logger.getLogger(ObjectNode.class.getName());


    public ObjectNode(String name) {
        super(name);
    }

    public int initPastry(int key) throws Exception {

        if (key == 0) this.nodeId = createKey();
        else this.nodeId = key;

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId);

        if (this.bootstrapNode != null) {
            System.out.println("Pastry Node " + this.nodeId + ": Joining network...");

            if (this.bootstrapNode == null) {
                System.out.println("Pastry Node " + this.nodeId + ": ERROR No known node to join Pastry network");
                return -1;
            }

            // Send a message to bootstrap node to join the Pastry network. Node that finally attends the join request
            // sends back their State tables
            //
            StateTable stateTable = ((ObjectNode) (this.bootstrapNode)).join(this);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN_RESPONSE (" + stateTable + ")" + ANSI_RESET);

            System.out.println("Pastry Node " + this.nodeId + ": Adding node " + ((ObjectNode) (this.bootstrapNode)).getNodeId() + " to Neighborhood Set");
            this.neighborhoodSet.put(((ObjectNode) (this.bootstrapNode)).getNodeId(), bootstrapNode);

            // Update state tables: Leaf Set and Neighborhood Set
            //
            updateLeafSet(stateTable.getL());
            updateNeighborhoodSet(stateTable.getM());

            // Communicate neighbors that a new node has joined
            //
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Communicate neighbours that node " + this.nodeId + " joined Pastry network..." + ANSI_RESET);
            joined();
        }

        return this.nodeId;
    }

    @Override
    public void closePastry() {
        // Do nothing: intentionally empty
    }

    @Override
    public String toString() {
        return "Pastry Node {" +
                "name='" + name + '\'' +
                ", " + ANSI_RED + "nodeId=" + this.nodeId + ANSI_RESET +
                ", " + ANSI_BLUE + "L=" + Arrays.toString(this.leafSet) + ANSI_RESET +
                ", M=" + this.neighborhoodSet.keySet() +
                '}';
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
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Sending BROADCAST_PUT_DATA to Leaf Set..." + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3}; // order to send BROADCAST_PUT_DATA

        for (int i : numbers) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                ObjectNode node = (ObjectNode) this.neighborhoodSet.get(nodeId);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Deliver PUT_DATA_REQUEST to nodeId " + node.getNodeId() + ANSI_RESET);
                PutDataRequest request = new PutDataRequest(this.toString(), key, data);

                if ((fwEnabled) && ((i == 0) || (i == 3))) {
                    request.setFw_flag(true);
                }

                node.putDataRequest(request);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Ended BROADCAST_PUT_DATA to Leaf Set..." + ANSI_RESET);
    }

    public void broadcastRemoveData(int key) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Sending BROADCAST_REMOVE_DATA to Leaf Set..." + ANSI_RESET);

        int[] numbers = {1, 2, 0, 3}; // order to send BROADCAST_REMOVE_DATA

        for (int i : numbers) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                ObjectNode node = (ObjectNode) this.neighborhoodSet.get(nodeId);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Deliver REMOVE_DATA_REQUEST to nodeId " + node.getNodeId() + ANSI_RESET);
                RemoveDataRequest request = new RemoveDataRequest(this.toString(), key);
                node.removeDataRequest(request);
            }
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_RED + ">>>> Ended BROADCAST_PUT_DATA to Leaf Set..." + ANSI_RESET);
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
    public void updateNeighborhoodSet(Hashtable ar_neighborhoodSet) {
        System.out.println("Pastry Node " + this.nodeId + ": " + "Adding nodes " + ar_neighborhoodSet.keySet() + " to Neighborhood Set...");

        this.neighborhoodSet.putAll(ar_neighborhoodSet);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Neighborhood Set updated " + ANSI_RESET + this);
    }

    @Override
    public void route(PastryMessage msg, int key) {
        // Any node A that receives a message M with destination address D routes the message by comparing D with its
        // own GUID A and with each of the GUIDs in its leaf set and forwarding M to the node amongst them that is
        // numerically closest to D.
    }

    @Override
    public Object get(int key) {
        return this.nodeId;
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
            stateTable = new StateTable();
            stateTable.setL(this.nodeId, this.leafSet);

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

    }

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

    public String getDataRequest(GetDataRequest request) {
        return null;
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

}
