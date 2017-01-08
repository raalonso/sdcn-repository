package es.dit.muirst.sdcn.dht.remote;

import java.io.*;
import java.util.Hashtable;

import es.dit.muirst.sdcn.dht.messaging.JoinRequest;
import es.dit.muirst.sdcn.dht.messaging.JoinResponse;
import es.dit.muirst.sdcn.dht.messaging.BroadcastState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SerializationTest {

    private static final int l = 2;

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void ser_join_request() throws Exception {

        JoinRequest request = new JoinRequest(12);
        System.out.println("Object class " + request);

        try {
            FileOutputStream fileOut = new FileOutputStream("/tmp/join.ser");

            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(request);
            out.close();
            fileOut.close();
            System.out.println("Serialized data is saved in /tmp/join.ser");

        } catch(IOException i) {
            i.printStackTrace();
        }

        JoinRequest request_rcv = null;

        try {
            FileInputStream fileIn = new FileInputStream("/tmp/join.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            request_rcv = (JoinRequest) in.readObject();
            in.close();
            fileIn.close();
        } catch(IOException i) {
            i.printStackTrace();
            return;
        } catch(ClassNotFoundException c) {
            System.out.println("Object class not found");
            c.printStackTrace();
            return;
        }

        System.out.println("RECEIVED Object class " + request_rcv);

    }

    @Test
    public void ser_join_response() throws Exception {

        int[] leafSet = {12, 20, 25, 30};
        JoinResponse join_response = new JoinResponse(23, leafSet);
        System.out.println("Object class " + join_response);

        try {
            FileOutputStream fileOut = new FileOutputStream("/tmp/join_response.ser");

            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(join_response);
            out.close();
            fileOut.close();
            System.out.println("Serialized data is saved in /tmp/join_response.ser");

        } catch(IOException i) {
            i.printStackTrace();
        }

        JoinResponse join_response_rcv = null;

        try {
            FileInputStream fileIn = new FileInputStream("/tmp/join_response.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            join_response_rcv = (JoinResponse) in.readObject();
            in.close();
            fileIn.close();
        } catch(IOException i) {
            i.printStackTrace();
            return;
        } catch(ClassNotFoundException c) {
            System.out.println("Object class not found");
            c.printStackTrace();
            return;
        }

        System.out.println("RECEIVED Object class " + join_response_rcv);

    }

    @Test
    public void ser_broadcast_state() throws Exception {
        int nodeId = 14;
        int[] leafSet = {12, 20, 25, 30};
        Hashtable neighborhoodSet = new Hashtable(l*2);
        neighborhoodSet.put(12, new Integer(1222));
        neighborhoodSet.put(20, new Integer(1222));
        neighborhoodSet.put(25, new Integer(1222));
        neighborhoodSet.put(30, new Integer(1222));

        BroadcastState broadcast_state = new BroadcastState(nodeId, leafSet, neighborhoodSet);
        System.out.println("Object class " + broadcast_state);

        try {
            FileOutputStream fileOut = new FileOutputStream("/tmp/broadcast_state.ser");

            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(broadcast_state);
            out.close();
            fileOut.close();
            System.out.println("Serialized data is saved in /tmp/broadcast_state.ser");

        } catch(IOException i) {
            i.printStackTrace();
        }

        BroadcastState broadcast_state_rcv = null;

        try {
            FileInputStream fileIn = new FileInputStream("/tmp/broadcast_state.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            broadcast_state_rcv = (BroadcastState) in.readObject();
            in.close();
            fileIn.close();
        } catch(IOException i) {
            i.printStackTrace();
            return;
        } catch(ClassNotFoundException c) {
            System.out.println("Object class not found");
            c.printStackTrace();
            return;
        }

        System.out.println("RECEIVED Object class " + broadcast_state_rcv);

    }
}
