package com.nebutown.cluster;

public class Node {

    private int id;

    public int getId() {

        return id;
    }

    public static class BrokerInfo {
        private Node node;
        public String path(){

            return ZNode.getPath(node.id);
        }
        public byte[] toJsonBytes(){

            return ZNode.encode(this);
        }

        public Node getBroker() {
            return node;
        }
    }

    public static class ZNode{
        public static String getPath() {

            throw new UnsupportedOperationException();
        }
        public static String getPath(int id) {

            throw new UnsupportedOperationException();
        }

        public static byte[] encode(BrokerInfo brokerInfo) {

            throw new UnsupportedOperationException();
        }

        public static BrokerInfo decode(Integer brokerId, byte[] data) {

            throw new UnsupportedOperationException();
        }

    }

}
