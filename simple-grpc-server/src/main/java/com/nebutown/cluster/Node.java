package com.nebutown.cluster;

import com.example.grpc.server.RID;

import java.util.UUID;

public class Node {

    private int id;

    public int getId() {

        return id;
    }

    public String getRecord(RID request, UUID key) {
        throw new UnsupportedOperationException();
    }

    public <T> void putRecord(RID request, UUID key, T data) {
        throw new UnsupportedOperationException();
    }

    public boolean tryCommit(RID request) {
        throw new UnsupportedOperationException();
    }

    public void commit(RID request) {
        throw new UnsupportedOperationException();
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
