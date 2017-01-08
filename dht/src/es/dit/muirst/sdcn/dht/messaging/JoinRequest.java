package es.dit.muirst.sdcn.dht.messaging;

public class JoinRequest extends Message {

    protected int nodeId;

    public JoinRequest(int nodeId) {
        this.request_type = JOIN_REQUEST;
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "nodeId=" + nodeId +
                '}';
    }
}
