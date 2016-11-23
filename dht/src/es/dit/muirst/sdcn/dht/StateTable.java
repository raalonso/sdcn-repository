package es.dit.muirst.sdcn.dht;

import java.util.Arrays;
import java.util.Hashtable;

public class StateTable {
    private static final int l = 2;

    Hashtable R;
    int[] L;

    public Hashtable getR() {
        return R;
    }

    public int[] getL() {
        return L;
    }

    public void setR(Hashtable routingTable) {
        this.R = (Hashtable) routingTable.clone();
    }

    public void setL(int nodeId, int[] leafSet) {
        L = new int[2*l+1];
        // Smaller
        L[0] = leafSet[0]; L[1] = leafSet[1];
        // This node
        L[2] = nodeId;
        // Larger
        L[3] = leafSet[2]; L[4] = leafSet[3];
    }

    @Override
    public String toString() {
        return "StateTable{" +
                "R=" + R +
                ", L=" + Arrays.toString(L) +
                '}';
    }
}

