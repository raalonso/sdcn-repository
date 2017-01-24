package es.dit.muirst.sdcn.dht.messaging;

public class RemoveDataRequest extends Message {
    protected int key;

    public RemoveDataRequest(String UUID, int key) {
        super(REMOVE_DATA_REQUEST, UUID);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "RemoveDataRequest{" +
                "key=" + key +
                '}';
    }
}
