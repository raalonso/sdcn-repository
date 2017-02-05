package es.dit.muirst.sdcn.dht.interfaces;

import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.messaging.PastryMessage;

import java.util.Hashtable;

public interface Node {

    // Init Pastry
    //
    int initPastry(int key) throws Exception;
    void closePastry();

    default void run() {}

    int getNodeId();

    // Core routing algorithm
    //
    void route(PastryMessage msg, int key);

    // Operations
    //
    StateTable join(Node node);
    void leave(Node fromNode, int nodeId, StateTable stateTable);
    void broadcastState(Node fromNode, StateTable stateTable);

    //
    void joined();

    //
    void onNodeLeave(int nodeId);

    // Internal operations
    //
    void updateLeafSet(int[] leafs);
    void updateNeighborhoodSet(Hashtable neighborhoodSet);

}
