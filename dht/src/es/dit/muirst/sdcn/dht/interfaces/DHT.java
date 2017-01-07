package es.dit.muirst.sdcn.dht.interfaces;


import es.dit.muirst.sdcn.dht.messaging.PutDataRequest;

public interface DHT<Data> {

    int getNodeId();

    /**
     * Stores data in replicas at all nodes responsible for the object identified by the key.
     *
     * @param key
     * @param data
     */
    void putData(int key, Data data);

    /**
     * Retrieves the data associated with the key from one of the nodes responsible for it.
     *
     * @param key
     * @return
     */
    Data getData(int key);

    /**
     * Deletes all references to GUID and the associated data.
     *
     * @param key
     */
    void removeData(int key);

    // Operations
    //
    void putDataRequest(PutDataRequest node);

    String printOutLocalDHT();

}
