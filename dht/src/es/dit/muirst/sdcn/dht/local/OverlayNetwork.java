package es.dit.muirst.sdcn.dht.local;

import es.dit.muirst.sdcn.dht.interfaces.Node;

import java.util.ArrayList;

public class OverlayNetwork {

    public ArrayList<Node> nodes = new ArrayList<Node>();

    public void add(Node node) {
        this.nodes.add(node);
    }

    public void printOutNetwork() {
        System.out.println("\n-- Overlay Network -----");
        nodes.forEach(System.out::println);
        System.out.println("------------------------\n");
    }

    @Override
    public String toString() {
        return "OverlayNetwork {" +
                "nodes=" + nodes +
                '}';
    }
}
