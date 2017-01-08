package es.dit.muirst.sdcn.dht.local;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Hashtable;

import static org.junit.Assert.*;


public class ObjectNodeTest {

    ObjectNode node;

    @Before
    public void setUp() throws Exception {
        this.node = new ObjectNode("test.node.dit.upm.es");
        node.setNodeId(53);
    }

    @After
    public void tearDown() throws Exception {
        this.node = null;
    }

    @Test
    public void updateLeafSet() throws Exception {

    }

    @Test
    public void updateNeighborhoodSet() throws Exception {

    }

    @Test
    public void distance() throws Exception {
        int d = node.distance(52, 140);
        System.out.println(d);
    }

    @Test
    public void calculateDistances() throws Exception {

    }

    @Test
    public void localDHT() throws Exception {

    }


    @Test
    public void isInLeafSet() throws Exception {
        boolean result;
        int newNodeId;

//        int[] leafSet = {120, 50, 60, 65};
//        node.leafSet = leafSet;
//        int[] leafDistances = {};


//        newNodeId = 54;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertTrue(result);
//
//        newNodeId = 51;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertTrue(result);
//
//        newNodeId = 49;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertTrue(result);
//
//        newNodeId = 63;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertTrue(result);
//
//        newNodeId = 100;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertFalse(result);
//
//        newNodeId = 125;
//        result = node.isInLeafSet(newNodeId, leafDistances);
//        assertTrue(result);

    }

}