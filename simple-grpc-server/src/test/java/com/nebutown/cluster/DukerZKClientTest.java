package com.nebutown.cluster;

import com.google.common.collect.ImmutableMap;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DukerZKClientTest {
    private DukerZKClient dukerZKClient;

    @Before
    public void setUp() throws Exception {
        dukerZKClient = new DukerZKClient(new ZKClient("127.0.0.1:2181", 1000*60*5, Integer.MAX_VALUE), false);

        final String cluster = dukerZKClient.createOrGetCluster("test01");
        Config.getInstance().setClusterName(cluster);
        dukerZKClient.makeSurePersistentPathsExists(ZKNode.NodesZNode.getPath(), ZKNode.BranchesZNode.path());

    }

    @After
    public void tearDown() throws Exception {
        if (dukerZKClient != null) {
            dukerZKClient.close();
        }
    }

    @Test
    public void registerBroker() {
        Node.NodeInfo nodeInfo = Config.getInstance().getNode();


        try {
            dukerZKClient.registerBroker(nodeInfo);
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        try {
            System.out.println(dukerZKClient.getSortedBrokerList());
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        try {
            Thread.currentThread().join(1000*60*5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}