package es.dit.muirst.sdcn.dht.messaging;

import java.util.Arrays;
import java.util.Hashtable;

public class BroadcastState extends Message {

    private static final int l = 2;

    int nodeId; // State Table of this nodeId
    Hashtable M; // Neighborhood set
    int[] L; // Leaf set


    public BroadcastState(int nodeId, String UUID, int[] l, Hashtable m) {
        super(BROADCAST_STATE, UUID);
        this.nodeId = nodeId;
        setL(nodeId, l);
        setM(m);
    }

    public int getNodeId() {
        return this.nodeId;
    }

    public Hashtable getM() {
        return this.M;
    }

    public int[] getL() {
        return this.L;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setM(Hashtable routingTable) {
        this.M = (Hashtable) routingTable.clone();
    }

    public void setL(int nodeId, int[] leafSet) {
        this.L = new int[2*l+1];

        // Smaller
        this.L[0] = leafSet[0];
        this.L[1] = leafSet[1];
        // This node
        this.L[2] = nodeId;
        // Larger
        this.L[3] = leafSet[2];
        this.L[4] = leafSet[3];
    }

    @Override
    public String toString() {
        return "BroadcastState{" +
                "nodeId=" + nodeId +
                ", M=" + M +
                ", L=" + Arrays.toString(L) +
                '}';
    }

}
