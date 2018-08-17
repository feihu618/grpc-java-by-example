package com.nebutown.cluster;


import com.google.common.collect.Lists;
import com.nebutown.cluster.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class ZKClientTest {

    Integer zkConnectionTimeout = 10000;
    Integer zkSessionTimeout = 6000;
    Integer zkMaxInFlightRequests = Integer.MAX_VALUE;

    protected Boolean zkAclsEnabled = false;

    private String mockPath = "/foo7";
//    private val time = Time.SYSTEM;


    ZKClient zkClient = null;
//    AdminZkClient adminZkClient:  = null;

//    var zookeeper: EmbeddedZookeeper = null;

    int zkPort = 2181;
    String zkConnect = "127.0.0.1:"+zkPort;

    @Before
    public void setUp() {

//        zookeeper = new EmbeddedZookeeper()
        zkClient = new ZKClient(zkConnect, zkSessionTimeout, zkMaxInFlightRequests);

    }

    @After
    public void tearDown() throws InterruptedException {
        if (zkClient != null)
            zkClient.close();

    }

    @Test
    public void  testConnection() {
        try {
            new ZKClient(zkConnect, zkSessionTimeout, Integer.MAX_VALUE).close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void  testGetDataExistingZNode() throws InterruptedException {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        ZKClient.AsyncResponse createResponse = zkClient.handleRequest(new ZKClient.CreateRequest(mockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, null));
        assertEquals("Response code for create should be OK", KeeperException.Code.OK, createResponse.getResultCode());
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) zkClient.handleRequest(new ZKClient.GetDataRequest(mockPath, null));
        assertEquals("Response code for getData should be OK", KeeperException.Code.OK, getDataResponse.getResultCode());
        assertArrayEquals("Data for getData should match created znode data", data, getDataResponse.getData());
    }



    @Test
    public void  testZNodeChangeHandlerForCreation() throws InterruptedException {

        CountDownLatch znodeChangeHandlerCountDownLatch = new CountDownLatch(1);
        ZKClient.ZNodeChangeHandler  zNodeChangeHandler = new ZKClient.ZNodeChangeHandler (){
            @Override public void  handleCreation(){
                znodeChangeHandlerCountDownLatch.countDown();
                System.out.println("--- handleCreation");
            }

            @Override
            public void handleDeletion() {
                System.out.println("--- handleDeletion");
            }

            @Override
            public void handleDataChange() {
                System.out.println("--- handleDataChange");
            }

            @Override public String   getPath(){ return mockPath;}
        };

        zkClient.registerZNodeChangeHandler(zNodeChangeHandler);
        ZKClient.ExistsRequest existsRequest = new ZKClient.ExistsRequest(mockPath, null);
        ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(mockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        List<ZKClient.AsyncResponse> responses = zkClient.handleRequests(Lists.newArrayList(existsRequest, createRequest));
        assertEquals("Response code for exists should be NONODE", KeeperException.Code.NONODE, responses.get(0).getResultCode());
        assertEquals("Response code for create should be OK", KeeperException.Code.OK, responses.get(responses.size() - 1).getResultCode());
        assertTrue("Failed to receive create notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS));
    }

    @org.junit.Test
    public void handleRequest() {
    }
}
