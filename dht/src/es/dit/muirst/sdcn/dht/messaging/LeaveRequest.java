package es.dit.muirst.sdcn.dht.messaging;


public class LeaveRequest extends PastryMessage {
    protected int nodeId;

    public LeaveRequest(int nodeId, String UUID) {
        super(LEAVE_REQUEST, UUID);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "LeaveRequest{" +
                "nodeId=" + nodeId +
                '}';
    }
}
