package com.nebutown.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private final DukerZKClient zkClient;


    public Cluster(DukerZKClient zkClient){
        this.zkClient = zkClient;
    }


    public void upgrade(Runnable task) {
        //lock
        throw new UnsupportedOperationException();
    }

    //consistent-hashing
    public Node getNode(String key) {
        throw new UnsupportedOperationException();
    }

}
