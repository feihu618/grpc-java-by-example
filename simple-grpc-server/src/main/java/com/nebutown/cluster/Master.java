package com.nebutown.cluster;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Master {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    private DukerZKClient zkClient;
    private Node.NodeInfo nodeInfo;
    private ZKClient.ZNodeChangeHandler masterChangeHandler = null;
    private int activeControllerId;
    private int epoch;
    private Config config;

    public Master(DukerZKClient dukerZKClient, Config config) {
        this.zkClient = dukerZKClient;
        this.config = config;
        this.nodeInfo = config.getNode();
        this.activeControllerId = -1;

    }

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
                    zkClient.registerBroker(nodeInfo);

                    elect();
                } catch (KeeperException e) {
                    e.printStackTrace();
                    throw new RuntimeException("--- Can't register masterChangeHandler~", e);
                }



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

    public boolean isMaster() {

        return activeControllerId == nodeInfo.getId();
    }

    public void reElect() {

    }

    void onElectSuccess() {
//        List<Integer> childrenIds = zkClient.getSortedBrokerList();
    }

    void onElectFail() {

    }

    void onMasterChange() {
        boolean wasMasterBeforeChange = isMaster();
        try {
            zkClient.registerZNodeChangeHandlerAndCheckExistence(masterChangeHandler);
        } catch (KeeperException e) {
            LOG.error("happer error when register handler:", e);
        }
        activeControllerId = zkClient.getControllerId().orElse(-1);
        if (wasMasterBeforeChange && !isMaster()) {
            onMasterResignation();
        }
    }

    void onChildChange() {

    }

    void onViewChanged() {
        //update epoch value
        epoch = -2;
        //get data from getDataPath
        //put record into BallotPath
        //register handler for CommitStat
    }

    void onBrachCommit() {

        //notify Cluster upgrade finish
    }

    private void onMasterResignation() {

    }


    class NodeChangeHandler implements ZKClient.ZNodeChildChangeHandler {

        @Override
        public String getPath() {
            return ZKNode.NodesZNode.getPath();
        }

        @Override
        public void handleChildChange() {

            onChildChange();
        }
    }


    //for slave
    class BranchCommitHandler implements ZKClient.ZNodeChangeHandler{

        @Override
        public String getPath() {
            return ZKNode.BranchesZNode.getCommitStatPath(epoch);
        }

        @Override
        public void handleCreation() {
            //ignore
        }

        @Override
        public void handleDeletion() {
            //ignore
        }

        @Override
        public void handleDataChange() {
            onBrachCommit();
        }
    }


    //for slave
    class EpochHandler implements ZKClient.ZNodeChangeHandler {

        @Override
        public String getPath() {
            return ZKNode.EpochZNode.path();
        }

        @Override
        public void handleCreation() {
            //ignore
        }

        @Override
        public void handleDeletion() {
            //TODO: recreate path
        }

        @Override
        public void handleDataChange() {
            onViewChanged();
        }
    }

    class MasterChangeHandler implements ZKClient.ZNodeChangeHandler {
        @Override
        public String getPath() {
            return ZKNode.MasterZNode.path();
        }

        @Override
        public void handleCreation() {
            onMasterChange();
        }

        @Override
        public void handleDeletion() {
            reElect();
        }

        @Override
        public void handleDataChange() {
            onMasterChange();
        }
    }






}
