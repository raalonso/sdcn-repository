package es.dit.muirst.sdcn.dht.remote;


import es.dit.muirst.sdcn.dht.PastryNode;
import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.interfaces.DHT;
import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.local.ObjectNode;
import es.dit.muirst.sdcn.dht.messaging.BroadcastState;
import es.dit.muirst.sdcn.dht.messaging.JoinRequest;
import es.dit.muirst.sdcn.dht.messaging.JoinResponse;
import es.dit.muirst.sdcn.dht.messaging.Message;
import org.jgroups.*;
import org.apache.commons.cli.*;
import org.jgroups.util.UUID;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class GroupNode extends PastryNode<org.jgroups.Address> implements Node, Receiver {

    private static final Logger LOGGER = Logger.getLogger(GroupNode.class.getName());

    JChannel channel;
    String addressAsUUID;

    String user_name = System.getProperty("user.name", "n/a");

    final List<String> state = new LinkedList<String>();


    public GroupNode(String name) {
        super(name);
    }

    @Override
    public int initPastry(int key) throws Exception {

        this.channel = new JChannel();

        this.channel.setReceiver(this);
        this.channel.connect("PastryRing");
        this.channel.getState(null, 10000);
        this.channel.setDiscardOwnMessages(true); // All messages sent by node A will be discarded by node A

        if (key == 0) this.nodeId = createKey();
        else this.nodeId = key;

        this.addressAsUUID = this.channel.getAddressAsUUID();
        System.out.println("Pastry Node " + this.nodeId + ": Pastry Node address " + ANSI_BLUE + this.addressAsUUID + ANSI_RESET);

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId + " with address UUID " + this.addressAsUUID);

        if (this.bootstrapNode != null) {
            System.out.println("Pastry Node " + this.nodeId + ": Joining network...");

            if (this.bootstrapNode == null) {
                System.out.println("Pastry Node " + this.nodeId + ": ERROR No known node to join Pastry network");
                return -1;
            }

            System.out.println("Pastry Node " + this.nodeId + ": Bootstrap Pastry Node address " + ANSI_BLUE + ((org.jgroups.Address) this.bootstrapNode) + ANSI_RESET);

            // Send a message to bootstrap node to join the Pastry network. Node that finally attends the join request
            // sends back their State tables
            //
            try {
                JoinRequest request = new JoinRequest(this.nodeId, this.addressAsUUID);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending JOIN_REQUEST to bootstrap node " + this.bootstrapNode + "..." + ANSI_RESET);

                Address dest = this.bootstrapNode;
                Address src = null; // address of sender

                org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

                this.channel.send(msg);

            } catch (Exception e) {
                e.printStackTrace();
            }

            // TODO: Add your code here...

        }
        else {
            String viewName = this.channel.getViewAsString();

            System.out.println("Pastry Node " + this.nodeId + ": Correctly added into view " + ANSI_BLUE + viewName + ANSI_RESET);
        }

        return this.nodeId;
    }

    @Override
    public void closePastry() {
        this.channel.close();
    }

    @Override
    public void run() {
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("PASTRY_NodeId-" + this.nodeId + "> "); System.out.flush();
                String line=in.readLine().toLowerCase();
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    break;
                }
            }
            catch(Exception e) {
            }
        }
    }

    @Override
    public void joined() {
        System.out.println("Pastry Node " + this.nodeId + ": Sending BROADCAST_STATE to neighbors...");

        for (int i = 0; i < this.leafSet.length; i++) {
            int nodeId = this.leafSet[i];
            if (nodeId != 0) {
                System.out.println("Pastry Node " + this.nodeId + ": >> Neighbour Pastry node " + nodeId);
                if (this.neighborhoodSet.containsKey(nodeId)) {
                    String neighborNodeUUID = (String) this.neighborhoodSet.get(nodeId);

                    BroadcastState request = new BroadcastState(this.nodeId, this.toString(), this.leafSet, this.neighborhoodSet);

                    System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending BROADCAST_STATE to neighbor " + nodeId + "..." + ANSI_RESET);

                    try {
                        Address dest = UUID.fromString(neighborNodeUUID);
                        Address src = null; // address of sender

                        org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

                        this.channel.send(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else {
                    System.out.println("Pastry Node " + this.nodeId + ": >> ERROR Neighbour Pastry node " + nodeId + " NOT in Set");
                }
            }
        }
    }

    @Override
    public org.jgroups.Address get(int key) {
        return null;
    }

    @Override
    public void route(Message msg, int key) {

    }

    @Override
    public StateTable join(Node node) {
        return null;
    }

    public void onJoinRequest(JoinRequest request) {

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN request from " + request.getNodeId() + ANSI_RESET);

        // Maps nodeId to address of node (in local is address of object)
        //
        System.out.println("Pastry Node " + this.nodeId + ": Adding node " + request.getNodeId() + " to local Routing Table");
        this.neighborhoodSet.put(request.getNodeId(), request.getSender_address());

        // Create stateTable to send to other nodes
        //
        StateTable stateTable = new StateTable();
        stateTable.setL(this.nodeId, this.leafSet);

        int newNodeId = request.getNodeId();
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

            try {
                JoinResponse join_response = new JoinResponse(this.nodeId, this.addressAsUUID, this.leafSet, this.neighborhoodSet);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending JOIN_RESPONSE to joined node " + request.getSender_address() + "..." + ANSI_RESET);

                Address dest = UUID.fromString(request.getSender_address());
                Address src = null; // address of sender

                org.jgroups.Message msg = new org.jgroups.Message(dest, src, join_response);

                this.channel.send(msg);

            } catch (Exception e) {
                e.printStackTrace();
            }


        } else {
            String fwdNodeUUID = (String) this.neighborhoodSet.get(routeToNodeId);

            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Route JOIN request to nodeId " + result[0] + " (with min distance of " + result[1] + ")" + ANSI_RESET);

            try {
                Address dest = UUID.fromString(fwdNodeUUID); // the message is sent to a member of the group
                Address src = null; // address of sender

                org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

                this.channel.send(msg);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        stateTable.setM(this.neighborhoodSet);

    }

    public void onJoinResponse(JoinResponse message) {

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN response from " + message.getNodeId() + ANSI_RESET);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN_RESPONSE (" + message + ")" + ANSI_RESET);

        System.out.println("Pastry Node " + this.nodeId + ": Adding node " + message.getNodeId() + " to Neighborhood Set");
        this.neighborhoodSet.put(message.getNodeId(), message.getSender_address());

        // Update state tables: Leaf Set and Neighborhood Set
        //
        updateLeafSet(message.getLeafSet());
        updateNeighborhoodSet(message.getNeighborhoodSet());

        // Communicate neighbors that a new node has joined
        //
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Communicate neighbours that node " + this.nodeId + " joined Pastry network..." + ANSI_RESET);
        joined();

    }

    public void onBroadcastState(BroadcastState message) {
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received JOIN response from " + message.getNodeId() + ANSI_RESET);

        int fromNodeId = message.getNodeId();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received BROADCAST_STATE from " + fromNodeId + ANSI_RESET + ": " + message);

        System.out.println("Pastry Node " + this.nodeId + ": " + "Updating Leaf Set...");
        updateLeafSet(message.getL());

        System.out.println("Pastry Node " + this.nodeId + ": " + "Updating Neighborhood Set...");
        updateNeighborhoodSet(message.getM());

    }

    @Override
    public void leave(Node fromNode, int nodeId, StateTable stateTable) {

    }

    @Override
    public void broadcastState(Node fromNode, StateTable stateTable) {
        // Do nothing: intentionally empty...
    }

    @Override
    public void onNodeLeave(int nodeId) {

    }

    @Override
    public void updateNeighborhoodSet(Hashtable neighborhoodSet) {
        System.out.println("Pastry Node " + this.nodeId + ": " + "Adding nodes " + neighborhoodSet.keySet() + " to Neighborhood Set...");

        this.neighborhoodSet.putAll(neighborhoodSet);

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_GREEN + "Neighborhood Set updated " + ANSI_RESET + this);
    }

    // Implements methods in org.jgroups.MembershipListener
    //

    @Override
    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    @Override
    public void suspect(Address address) {

    }

    @Override
    public void block() {

    }

    @Override
    public void unblock() {

    }

    // Implements methods in org.jgroups.MessageListener
    //

    @Override
    public void receive(org.jgroups.Message msg) {
        Message request = (Message) msg.getObject();
        String fromNodeId = msg.getSrc().toString();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received request " + request + " from nodeId " + fromNodeId + "..." + ANSI_RESET);

        if (request.getRequest_type() == Message.JOIN_REQUEST) {
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Handle JOIN_REQUEST..." + ANSI_RESET);

            JoinRequest join_request = (JoinRequest) request;
            onJoinRequest(join_request);
        }
        else if (request.getRequest_type() == Message.JOIN_RESPONSE) {
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Handle JOIN_RESPONSE..." + ANSI_RESET);

            JoinResponse join_response = (JoinResponse) request;
            onJoinResponse(join_response);
        }
        else if (request.getRequest_type() == Message.BROADCAST_STATE) {
            System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Handle JOIN_RESPONSE..." + ANSI_RESET);

            BroadcastState broadcast_state = (BroadcastState) request;
            onBroadcastState(broadcast_state);
        }

        System.out.println("Pastry Node " + this.nodeId + ": " + this);

    }

    @Override
    public void getState(OutputStream outputStream) throws Exception {

    }

    @Override
    public void setState(InputStream inputStream) throws Exception {

    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option nodeName_opt = new Option("n", "node-name", true, "name of the node");
        nodeName_opt.setRequired(true);
        options.addOption(nodeName_opt);

        Option nodeId_opt = new Option("i", "node-id", true, "node identifier in Pastry network");
        nodeId_opt.setRequired(true);
        options.addOption(nodeId_opt);

        Option bootstrapnode_opt = new Option("b", "bootstrap-node-id", true, "node Id of bootstrap node");
        bootstrapnode_opt.setRequired(false);
        options.addOption(bootstrapnode_opt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("pastryNode", options);

            System.exit(1);
            return;
        }


        String nodeName = cmd.getOptionValue("n");
        int nodeId = Integer.parseInt(cmd.getOptionValue("i"));
        String bootstrapNodeId = cmd.getOptionValue("b");

        UUID bootstrapNode_uuid = null;
        if (cmd.hasOption("b")) {
            bootstrapNode_uuid = UUID.fromString(bootstrapNodeId);
        }

        PastryNode node = new GroupNode(nodeName);
        node.setBootstrapNode(bootstrapNode_uuid);
        nodeId = node.initPastry(nodeId);
        System.out.println(node);
        node.run();
        node.closePastry();

    }



}
