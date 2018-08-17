package com.nebutown.cluster;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    private DukerZKClient zkClient;
    private Node.NodeInfo nodeInfo;
    private ZKClient.ZNodeChangeHandler masterChangeHandler = null;
    Integer activeControllerId;
    private Config config;

    public void startUp() {

        zkClient.registerSessionStateChangeHandler(new ZKClient.SessionStateChangeHandler() {
            @Override
            public String getName() {
                return "Master";
            }

            @Override
            public void beforeInitializingSession() {
                //TODO: clean
            }

            @Override
            public void afterInitializingSession() {
                try {
                    zkClient.registerZNodeChangeHandlerAndCheckExistence(masterChangeHandler);
                } catch (KeeperException e) {
                    e.printStackTrace();
                    throw new RuntimeException("--- Can't register masterChangeHandler~", e);
                }

                try {
                    zkClient.registerBroker(nodeInfo);
                } catch (KeeperException e) {
                    LOG.error("--- registerBroker:{} failed", nodeInfo, e);
                    throw new RuntimeException("registerBroker failed, please check configuration~");
                }

                elect();
            }
        });
    }

    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    public void elect() {
        long timestamp = System.currentTimeMillis();

        try {
            activeControllerId = zkClient.getControllerId().orElse(-1);
        /*
         * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
         * it's possible that the controller has already been elected when we get here. This check will prevent the following
         * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
         */
        if (activeControllerId != -1) {
            LOG.debug("Broker $activeControllerId has been elected as the controller, so stopping the election process.");
            onElectFail();
            return;
        }

            zkClient.registerController(config.getNodeId(), timestamp);
            LOG.info("${config.nodeId} successfully elected as the controller");
            activeControllerId = config.getNodeId();
            onElectSuccess();
        } catch (KeeperException.NodeExistsException e){
                // If someone else has written the path, then
                activeControllerId = zkClient.getControllerId().orElse(-1);

                if (activeControllerId != -1)
                    LOG.debug("Broker $activeControllerId was elected as controller instead of broker ${config.nodeId}");
                else
                    LOG.warn("A controller has been elected but just resigned, this will result in another round of election");

        } catch (Throwable t){


                LOG.error("Error while electing or becoming controller on broker ${config.nodeId}", t);
//                triggerControllerMove();
        }
    }

    public void reElect() {

    }

    void onElectSuccess() {

    }

    void onElectFail() {

    }





}
