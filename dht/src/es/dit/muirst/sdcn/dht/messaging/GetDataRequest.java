package es.dit.muirst.sdcn.dht.messaging;


public class GetDataRequest extends Message {
    protected int key;

    public GetDataRequest(int key) {
        this.request_type = GET_DATA_REQUEST;
        this.key = key;
    }

    public int getKey() {
        return key;
    }

}
