package es.dit.muirst.sdcn.dht.messaging;

public class PutDataRequest {
    protected int key;
    protected String data;

    protected boolean fw_flag;

    public PutDataRequest(int key, String data) {
        this.key = key;
        this.data = data;
        fw_flag = false;
    }

    public int getKey() {
        return key;
    }

    public String getData() {
        return data;
    }

    public boolean isFw_flag() {
        return fw_flag;
    }

    public void setFw_flag(boolean fw_flag) {
        this.fw_flag = fw_flag;
    }

    @Override
    public String toString() {
        return "PutDataRequest{" +
                "key=" + key +
                ", data='" + data + '\'' +
                ", fw_flag=" + fw_flag +
                '}';
    }
}
