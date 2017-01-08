package es.dit.muirst.sdcn.dht.messaging;

import java.io.Serializable;

public abstract class Message implements Serializable {

    public static final int JOIN_REQUEST = 0x01;
    public static final int JOIN_RESPONSE = 0x02;
    public static final int BROADCAST_STATE = 0x03;

    public static final int PUT_DATA_REQUEST = 0x04;
    public static final int REMOVE_DATA_REQUEST = 0x05;
    public static final int GET_DATA_REQUEST = 0x06;

    protected int request_type;

    protected int sender;

    public int getRequest_type() {
        return request_type;
    }

    public int getSender() {
        return sender;
    }

}
