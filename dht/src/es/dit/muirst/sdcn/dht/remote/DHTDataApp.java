package es.dit.muirst.sdcn.dht.remote;

import es.dit.muirst.sdcn.dht.interfaces.DHT;
import es.dit.muirst.sdcn.dht.messaging.*;
import org.apache.commons.cli.*;
import org.jgroups.*;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class DHTDataApp extends ReceiverAdapter implements DHT<String> {

    JChannel channel;
    String addressAsUUID;
    String pastry_node_address;
    String user_name = System.getProperty("user.name", "n/a");
    final List<String> state = new LinkedList<String>();


    public DHTDataApp(String address) {
        this.pastry_node_address = address;
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void receive(Message msg) {
        String line=msg.getSrc() + ": " + msg.getObject();
        System.out.println(line);
        synchronized(state) {
            state.add(line);
        }
    }

    public void getState(OutputStream output) throws Exception {
//        synchronized(state) {
//            Util.objectToStream(state, new DataOutputStream(output));
//        }
    }

    @SuppressWarnings("unchecked")
    public void setState(InputStream input) throws Exception {
//        List<String> list=(List<String>)Util.objectFromStream(new DataInputStream(input));
//        synchronized(state) {
//            state.clear();
//            state.addAll(list);
//        }
//        System.out.println("received state (" + list.size() + " messages in chat history):");
//        for(String str: list) {
//            System.out.println(str);
//        }
    }


    private void start() throws Exception {
        channel=new JChannel();
        channel.setReceiver(this);
        channel.connect("PastryRing");
        channel.getState(null, 10000);

        channel.getAddress();
        String addressAsString = channel.getAddressAsString();
        System.out.println("channel.getAddressAsString()" + addressAsString);
        this.addressAsUUID = channel.getAddressAsUUID();
        System.out.println("channel.getAddressAsUUID()" + this.addressAsUUID);

        eventLoop();
        channel.close();
    }

    private void eventLoop() {
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("> "); System.out.flush();
                String line=in.readLine().toLowerCase();
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    break;
                }
                else {
                    String[] tokens = line.split(":");
                    int key = Integer.parseInt(tokens[1]);
                    if (tokens[0].equals("put")) {
                        String data = tokens[2];
                        System.out.print("PUT operation {key=" + key + ";data=" + data + "}"); System.out.flush();

                        putData(key, data);

                    } else if (tokens[0].equals("remove")) {
                        System.out.print("REMOVE operation {key=" + key + "}"); System.out.flush();

                        removeData(key);

                    } else if (tokens[0].equals("get")) {
                        System.out.print("GET operation {key=" + key + "}"); System.out.flush();

                        getData(key);
                    }
                }

            }
            catch(Exception e) {
            }
        }
    }

    @Override
    public void putData(int key, String data) {
        PutDataRequest request = new PutDataRequest(this.addressAsUUID, key, data);

        try {
            Address dest = UUID.fromString(this.pastry_node_address);
            Address src = null; // address of sender

            org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

            this.channel.send(msg);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public String getData(int key) {
        GetDataRequest request = new GetDataRequest(key, this.addressAsUUID);

        try {
            Address dest = UUID.fromString(this.pastry_node_address);
            Address src = null; // address of sender

            org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

            this.channel.send(msg);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void removeData(int key) {
        RemoveDataRequest request = new RemoveDataRequest(this.addressAsUUID, key);

        try {
            Address dest = UUID.fromString(this.pastry_node_address);
            Address src = null; // address of sender

            org.jgroups.Message msg = new org.jgroups.Message(dest, src, request);

            this.channel.send(msg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option uuid_opt = new Option("u", "uuid", true, "UUID of a node in Pastry Ring");
        uuid_opt.setRequired(false);
        options.addOption(uuid_opt);

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

        // Read config parameters
        //
        String uuid = cmd.getOptionValue("u");

        DHTDataApp app = new DHTDataApp(uuid);
        app.start();
    }
}
