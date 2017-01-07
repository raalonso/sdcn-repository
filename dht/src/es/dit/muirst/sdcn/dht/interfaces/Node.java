package es.dit.muirst.sdcn.dht.interfaces;

import es.dit.muirst.sdcn.dht.StateTable;
import es.dit.muirst.sdcn.dht.messaging.Message;

import java.util.Hashtable;

public interface Node <Address> {

    // Init Pastry
    //
    int initPastry(Node bootstrapNode, int key);

    int getNodeId();
    Address get(int key);

    // Core routing algorithm
    //
    void route(Message msg, int key);

    // Operations
    //
    StateTable join(Node node);
    void leave(Node fromNode, int nodeId, StateTable stateTable);
    void broadcastState(Node fromNode, StateTable stateTable);

    //
    void onNodeLeave(int nodeId);

    // Internal operations
    //
    void updateLeafSet(int[] leafs);
    void updateNeighborhoodSet(Hashtable neighborhoodSet);

}
