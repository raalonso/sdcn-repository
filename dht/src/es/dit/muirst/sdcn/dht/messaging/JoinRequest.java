package es.dit.muirst.sdcn.dht.messaging;

public class JoinRequest extends Message {

    protected int nodeId;

    public JoinRequest(int nodeId, String UUID) {
        super(JOIN_REQUEST, UUID);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "nodeId=" + nodeId +
                '}';
    }
}
