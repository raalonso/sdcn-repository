package es.dit.muirst.sdcn.dht.messaging;

import java.util.Arrays;
import java.util.Hashtable;

public class BroadcastNodeDeparture extends PastryMessage {
    private static final int l = 2;

    protected int nodeId; // nodeId of the left node
    protected int sender_nodeId;
    protected int[] L; // Leaf set
    protected Hashtable M; // Neighborhood set
    protected boolean only_notification = false;

    public BroadcastNodeDeparture(int sender_nodeId, int nodeId, String UUID, int[] leafSet, Hashtable m) {
        super(BROADCAST_NODE_DEPARTURE, UUID);
        this.nodeId = nodeId;
        this.sender_nodeId = sender_nodeId;
        setLeafSet(nodeId, leafSet);
        setNeighborhoodSet(m);
    }

    public int getNodeId() {
        return nodeId;
    }

    public int[] getLeafSet() {
        return L;
    }

    public Hashtable getNeighborhoodSet() {
        return M;
    }

    public void setNeighborhoodSet(Hashtable routingTable) {
        this.M = (Hashtable) routingTable.clone();
    }

    public void setLeafSet(int nodeId, int[] leafSet) {
        this.nodeId = nodeId;

        this.L = new int[2*l+1];
        // Smaller
        this.L[0] = leafSet[0];
        this.L[1] = leafSet[1];
        // This node
        this.L[2] = sender_nodeId;
        // Larger
        this.L[3] = leafSet[2];
        this.L[4] = leafSet[3];
    }

    public void setOnly_notification(boolean only_notification) {
        this.only_notification = only_notification;
    }

    public boolean isOnly_notification() {
        return only_notification;
    }

    @Override
    public String toString() {
        return "BroadcastNodeDeparture{" +
                "nodeId=" + nodeId +
                ", sender_nodeId=" + sender_nodeId +
                ", L=" + Arrays.toString(L) +
                ", M=" + M +
                ", only_notification=" + only_notification +
                '}';
    }
}
