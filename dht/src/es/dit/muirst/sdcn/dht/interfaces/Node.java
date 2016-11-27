package es.dit.muirst.sdcn.dht.interfaces;

import es.dit.muirst.sdcn.dht.StateTable;
import java.util.Hashtable;

public interface Node <Address> {

    // Init Pastry
    //
    int initPastry(Node nearbyNode, int key);

    int getNodeId();
    Address get(int key);

    // Core routing algorithm
    //
    void route(String msg, int key);

    // Operations
    //
    StateTable join(Node node);
    void leave();
    void broadcastState(Node fromNode, StateTable stateTable);

    // Internal operations
    //
    void updateLeafSet(int[] leafs);
    void updateNeighborhoodSet(Hashtable neighborhoodSet);

}
