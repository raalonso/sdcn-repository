package es.dit.muirst.sdcn.dht.messaging;


public class BroadcastRemoveData extends PastryMessage {
    protected int key;

    public BroadcastRemoveData(String sender_UUID, int key) {
        super(BROADCAST_REMOVE_DATA, sender_UUID);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "BroadcastRemoveData{" +
                "key=" + key +
                '}';
    }
}
