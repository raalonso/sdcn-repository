package es.dit.muirst.sdcn.dht.remote;


import es.dit.muirst.sdcn.dht.PastryNode;
import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.interfaces.DHT;
import es.dit.muirst.sdcn.dht.interfaces.Node;
import es.dit.muirst.sdcn.dht.messaging.BroadcastState;
import es.dit.muirst.sdcn.dht.messaging.JoinRequest;
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

        // LOGGER.info("INIT Pastry for node " + this.nodeId);
        System.out.println("INIT Pastry for node " + this.nodeId);

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
                JoinRequest request = new JoinRequest(this.nodeId);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending JOIN_REQUEST to bootstrap node " + this.bootstrapNode + "..." + ANSI_RESET);

                Address dest = null; // the message is sent to the group
                Address src = null; // address of sender

                org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

                this.channel.send(msg);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else {
            String viewName = this.channel.getViewAsString();

            System.out.println("Pastry Node " + this.nodeId + ": Correctly added into view " + ANSI_BLUE + viewName + ANSI_RESET);
        }

        String addressAsUUID = this.channel.getAddressAsUUID();
        System.out.println("Pastry Node " + this.nodeId + ": Pastry Node address " + ANSI_BLUE + addressAsUUID + ANSI_RESET);

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
                Node node = (Node) this.neighborhoodSet.get(nodeId);

                BroadcastState request = new BroadcastState(this.nodeId, this.leafSet, this.neighborhoodSet);

                System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Sending BROADCAST_STATE to neighbor " + nodeId + "..." + ANSI_RESET);

                try {
                    Address dest = null; // the message is sent to the group
                    Address src = null; // address of sender

                    org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

                    this.channel.send(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public int getNodeId() {
        return 0;
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
        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received request " + msg.getObject() + " from nodeId " + msg.getSrc() + "..." + ANSI_RESET);

//        String request = msg.getSrc() + ": " + msg.getObject();

        Message request = (Message) msg.getObject();
        String fromNodeId = msg.getSrc().toString();

        System.out.println("Pastry Node " + this.nodeId + ": " + ANSI_BLUE + "Received request " + request + " from nodeId " + fromNodeId + "..." + ANSI_RESET);

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
        node.run();
        node.closePastry();

    }



}
