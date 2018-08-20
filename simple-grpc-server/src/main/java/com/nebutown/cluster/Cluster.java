package com.nebutown.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final Cluster INSTANCE = new Cluster();



    public static Cluster getInstance() {
        return INSTANCE;
    }


    void pause() {
        throw new UnsupportedOperationException();
    }

    public void upgrade(List<Node> task) {
        //lock
        throw new UnsupportedOperationException();
    }

    //consistent-hashing
    public Node getNode(String key) {
        throw new UnsupportedOperationException();
    }

}
