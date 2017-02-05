package es.dit.muirst.sdcn.dht.messaging;

public class BroadcastPutData extends PastryMessage {
    protected int key;
    protected String data;

    public BroadcastPutData(String sender_UUID, int key, String data) {
        super(BROADCAST_PUT_DATA, sender_UUID);
        this.key = key;
        this.data = data;
    }

    public int getKey() {
        return key;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "BroadcastPutData{" +
                "key=" + key +
                ", data='" + data + '\'' +
                '}';
    }
}
