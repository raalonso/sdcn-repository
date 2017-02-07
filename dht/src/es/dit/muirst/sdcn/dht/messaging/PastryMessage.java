package es.dit.muirst.sdcn.dht.messaging;

import java.io.Serializable;

public abstract class PastryMessage implements Serializable {

    public static final int JOIN_REQUEST = 0x01;
    public static final int JOIN_RESPONSE = 0x02;
    public static final int BROADCAST_STATE = 0x03;

    public static final int PUT_DATA_REQUEST = 0x04;
    public static final int REMOVE_DATA_REQUEST = 0x05;
    public static final int GET_DATA_REQUEST = 0x06;
    public static final int GET_DATA_RESPONSE = 0x07;
    public static final int BROADCAST_PUT_DATA = 0x08;
    public static final int BROADCAST_REMOVE_DATA = 0x09;

    public static final int LEAVE_REQUEST = 0x0A;
    public static final int BROADCAST_NODE_DEPARTURE = 0x0B;

    protected int request_type;

    protected String sender_address;

    public PastryMessage(int request_type, String sender) {
        this.request_type = request_type;
        this.sender_address = sender;
    }

    public int getRequest_type() {
        return request_type;
    }

    public String getSender_address() {
        return sender_address;
    }

}
