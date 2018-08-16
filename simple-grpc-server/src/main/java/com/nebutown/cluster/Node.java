package com.nebutown.cluster;

public class Node {

    private int id;

    public int getId() {

        return id;
    }

    public static class NodeInfo {
        private Node node;
        public String path(){

            return ZKNode.NodesZNode.getPath(node.id);
        }
        public byte[] toJsonBytes(){

            return ZKNode.NodesZNode.encode(this);
        }

        public Node getBroker() {
            return node;
        }
    }

}
