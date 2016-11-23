package es.dit.muirst.sdcn.dht.interfaces;

import es.dit.muirst.sdcn.dht.StateTable;

import java.util.Hashtable;

public interface Node <Address> {

    // Init Pastry (simplified)
    //
    int initPastry(Node nearbyNode, int key);

    int getKey();
    Address get(int key);

    // Core routing algorithm
    //
    void route(String msg, int key);

    // Operations
    //
    StateTable join(Node node);
    void leave();
    void updateState(Node fromNode, StateTable stateTable);

    // Internal operations
    //
    void updateLeafSet(int[] leafs);
    void updateRoutingTable(Hashtable routingTable);

}
