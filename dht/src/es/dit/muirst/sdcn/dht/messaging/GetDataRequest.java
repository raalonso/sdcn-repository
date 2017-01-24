package es.dit.muirst.sdcn.dht.messaging;


public class GetDataRequest extends Message {
    protected int key;

    public GetDataRequest(int key, String UUID) {
        super(GET_DATA_REQUEST, UUID);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

}
