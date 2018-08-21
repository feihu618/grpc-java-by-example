package com.nebutown.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final Cluster INSTANCE = new Cluster();

    private int virtualNum = 5;//from config file
    private volatile TreeMap<Integer, Node> nodeMap = null;

    public static Cluster getInstance() {
        return INSTANCE;
    }


    void pause() {
        //TODO: whether lock Cluster or not
    }

    void upgrade(List<Node> task) {

        TreeMap<Integer, Node> tmp = buildHashCircle(task);

        nodeMap = tmp;
    }

    //consistent-hashing
    public Node getNode(String key) {

        final int hash = hash(key);
        Integer target = hash;
        if (!nodeMap.containsKey(hash)) {
            target = nodeMap.ceilingKey(hash);
            if (target == null && !nodeMap.isEmpty()) {
                target = nodeMap.firstKey();
            }
        }
        return nodeMap.get(target);
    }

    TreeMap<Integer, Node> buildHashCircle(List<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("--- supply valid argument for buildHashCircle");

        TreeMap<Integer, Node> nodeTreeMap = new TreeMap<>();
        for (Node node : nodes) {
            for (int i = 0; i < virtualNum; i++) {
                int nodeKey = hash(node.toString() + "-" + i);
                nodeTreeMap.put(nodeKey, node);
            }
        }

        return nodeTreeMap;
    }


    int hash(String key) {

        int hashCode = key.hashCode();
        return MurmurHash3.fmix32(hashCode);
    }


}
