package es.dit.muirst.sdcn.dht.messaging;

import java.util.Arrays;
import java.util.Hashtable;

public class JoinResponse extends PastryMessage {

    private static final int l = 2;

    protected int nodeId; // nodeId of the node that attended the join request
    protected int[] L; // Leaf set
    protected Hashtable M; // Neighborhood set


    public JoinResponse(int nodeId, String UUID, int[] leafSet, Hashtable m) {
        super(JOIN_RESPONSE, UUID);
        this.nodeId = nodeId;
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
        this.L[2] = nodeId;
        // Larger
        this.L[3] = leafSet[2];
        this.L[4] = leafSet[3];
    }

    @Override
    public String toString() {
        return "JoinResponse{" +
                "nodeId=" + nodeId +
                ", L=" + Arrays.toString(L) +
                ", M=" + M +
                '}';
    }
}
