package es.dit.muirst.sdcn.dht.messaging;

public class GetDataResponse extends PastryMessage {
    protected int key;
    protected String data;

    public GetDataResponse(String sender_UUID, int key, String data) {
        super(GET_DATA_RESPONSE, sender_UUID);
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
        return "GetDataResponse{" +
                "key=" + key +
                ", data='" + data + '\'' +
                '}';
    }
}
