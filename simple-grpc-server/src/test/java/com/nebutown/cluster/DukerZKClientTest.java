package com.nebutown.cluster;

import com.google.common.collect.ImmutableMap;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DukerZKClientTest {
    private DukerZKClient dukerZKClient;

    @Before
    public void setUp() throws Exception {
        dukerZKClient = new DukerZKClient(new ZKClient("127.0.0.1:2181", 1000*60*5, Integer.MAX_VALUE), false);

        dukerZKClient.createRecursive(ZKNode.ClusterZNode.getBasePath(), new byte[]{1, 23, 45, 67}, false);
        dukerZKClient.checkedEphemeralCreate(ZKNode.ClusterZNode.path("tmp_space"), new byte[0]);
        dukerZKClient.checkedEphemeralCreate(ZKNode.NodesZNode.getPath(), new byte[0]);
    }

    @After
    public void tearDown() throws Exception {
        if (dukerZKClient != null) {
            dukerZKClient.close();
        }
    }

    @Test
    public void registerBroker() {
        Node.NodeInfo nodeInfo = new Node.NodeInfo();

        nodeInfo.setId(1);
        nodeInfo.setHost("localhost");
        nodeInfo.setPort(8989);
        nodeInfo.setJmxPort(9001);
        nodeInfo.setOption(ImmutableMap.of("auth","123456"));
        nodeInfo.setVersion(1);


        try {
            dukerZKClient.registerBroker(nodeInfo);
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}