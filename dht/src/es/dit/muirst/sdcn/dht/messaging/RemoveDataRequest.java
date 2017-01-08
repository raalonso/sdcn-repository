package es.dit.muirst.sdcn.dht.messaging;

public class RemoveDataRequest extends Message {
    protected int key;

    public RemoveDataRequest(int key) {
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
