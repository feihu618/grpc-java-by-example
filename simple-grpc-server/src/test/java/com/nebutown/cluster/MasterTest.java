package com.nebutown.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MasterTest {
    private Master master;

    @Before
    public void setUp() throws Exception {
        DukerZKClient dukerZKClient = new DukerZKClient(new ZKClient("127.0.0.1:2181", 1000*60, Integer.MAX_VALUE), false);

        final String cluster = dukerZKClient.createOrGetCluster("test01");
        Config.getInstance().setClusterName(cluster);
        dukerZKClient.makeSurePersistentPathsExists(ZKNode.NodesZNode.getPath(), ZKNode.BranchesZNode.path());

        master = new Master(dukerZKClient, Config.getInstance());
    }

    @After
    public void tearDown() throws Exception {

        master.shutdown();
    }

    @Test
    public void startUp() {

        master.startUp();

        try {
            Thread.currentThread().join(1000*60*3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}