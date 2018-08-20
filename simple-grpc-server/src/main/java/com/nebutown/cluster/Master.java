package com.nebutown.cluster;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;

public class Master {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    private final ScheduledExecutorService eventDispatcher;
    private DukerZKClient zkClient;
    private Node.NodeInfo nodeInfo;
    private ZKClient.ZNodeChangeHandler masterChangeHandler = null;
    private ZKClient.ZNodeChangeHandler epochHandler = null;
    private ZKClient.ZNodeChangeHandler branchCommitHandler = null;
    private volatile CompletableFuture<Void> ballotCountFuture = null;
    private ArrayList<Integer> nodeIds = null;
    private int activeControllerId;
    private int epoch;
    private Config config;

    public Master(DukerZKClient dukerZKClient, Config config) {
        this.eventDispatcher = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("Event-Dispatcher-%d").build());
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
                    zkClient.registerZNodeChangeHandlerAndCheckExistence(epochHandler);
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
            activeControllerId = zkClient.getMasterId().orElse(-1);
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

            zkClient.registerMaster(config.getNodeId(), timestamp);
            LOG.info("${config.nodeId} successfully elected as the controller");
            activeControllerId = config.getNodeId();
            onElectSuccess();
        } catch (KeeperException.NodeExistsException e){
                // If someone else has written the path, then
                activeControllerId = zkClient.getMasterId().orElse(-1);

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
        try {

            List<Integer> childrenIds = zkClient.getSortedBrokerList();
            Optional<Tuple<Integer, Stat>> epoch = zkClient.getMasterEpoch();

            incrementMasterEpoch();

        } catch (KeeperException e) {

            //TODO: if SessionExpire, ignore, others throw IllegalStateException
            LOG.warn("--- onElectSuccess happen KeeperException ", e);
        } catch (Throwable t) {
            //TODO:
            LOG.warn("--- onElectSuccess happen Throwable ", t);
        }
    }

    private void incrementMasterEpoch() throws KeeperException {

        //TODO: incrementMasterEpoch
        List<Integer> childrenIds = zkClient.getSortedBrokerList();
        createBranchAndCommit(childrenIds);
        throw new UnsupportedOperationException();
    }

    private void createBranchAndCommit(List<Integer> childrenIds) throws KeeperException {

        zkClient.createBranch(epoch, childrenIds, System.currentTimeMillis());

        //poll count of children
        ballotCountFuture = scheduleAtFixedRate(()->{

            int checkedSize = -1;
            if (childrenIds.size() == checkedSize){

                try {

                    zkClient.commitBranch(epoch);
                    if (ballotCountFuture != null)
                        ballotCountFuture.cancel(false);
                } catch (KeeperException e) {
//                    e.printStackTrace();
                    LOG.warn("--- commitBranch happen exception:", e);
                }
            }

        }, 200);

        within(ballotCountFuture, 1000*60).handle((v, th) -> {

            if (th instanceof TimeoutException) {

                eventDispatcher.schedule(Master.this::onIncrementMasterEpochFailed, 50, TimeUnit.MILLISECONDS);
            }

            return null;
        });

    }

    void onIncrementMasterEpochFailed() {
        try {
            incrementMasterEpoch();
        } catch (KeeperException e) {
//            e.printStackTrace();
            LOG.warn("--- incrementMasterEpoch happen exception:", e);
        }
    }

    void onElectFail() {

        try {
            Optional<Tuple<Integer, Stat>> epoch = zkClient.getMasterEpoch();
            zkClient.registerZNodeChangeHandler(branchCommitHandler);//
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void onMasterChange() {
        boolean wasMasterBeforeChange = isMaster();
        try {
            zkClient.registerZNodeChangeHandlerAndCheckExistence(masterChangeHandler);
        } catch (KeeperException e) {
            LOG.error("happen error when register handler:", e);
        }
        activeControllerId = zkClient.getMasterId().orElse(-1);
        if (wasMasterBeforeChange && !isMaster()) {
            onMasterResignation();
        }
    }

    void onNodeChange() {

        //TODO: performance
        try {
            incrementMasterEpoch();
        } catch (KeeperException e) {
            LOG.error("--- onNodeChange happen error when register handler:", e);
        }
    }

    void onViewChanged() {
        //update epoch value
        epoch = -2;
        Cluster.getInstance().pause();

    }

    void onBranchCreated() {


        try {
            //get data from getDataPath
            nodeIds = zkClient.getBranchData(epoch);
            //put record into BallotPath
            zkClient.updateBranch(epoch, nodeInfo.getId());
        } catch (KeeperException e) {
            LOG.error("--- onNodeChange happen error when register handler:", e);
        }

    }

    void onBranchCommit() {

        //notify Cluster upgrade finish
        try {

            List<Node> nodes = zkClient.getAllNodesInCluster(() -> nodeIds);
            Cluster.getInstance().upgrade(nodes);

            //unregister handler for CommitStat
            zkClient.unregisterZNodeChangeHandler(ZKNode.BranchesZNode.getCommitStatPath(epoch));
        } catch (KeeperException e) {
            LOG.error("--- onNodeChange happen error when register handler:", e);
        }

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
            eventDispatcher.schedule(Master.this::onNodeChange, 0, TimeUnit.MILLISECONDS);
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

            eventDispatcher.schedule(Master.this::onBranchCreated, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public void handleDeletion() {
            //ignore
        }

        @Override
        public void handleDataChange() {

            eventDispatcher.schedule(Master.this::onBranchCommit, 0, TimeUnit.MILLISECONDS);
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
            eventDispatcher.schedule(Master.this::onViewChanged, 0, TimeUnit.MILLISECONDS);
        }
    }

    class MasterChangeHandler implements ZKClient.ZNodeChangeHandler {
        @Override
        public String getPath() {
            return ZKNode.MasterZNode.path();
        }

        @Override
        public void handleCreation() {
            eventDispatcher.schedule(Master.this::onMasterChange, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public void handleDeletion() {
            eventDispatcher.schedule(Master.this::reElect, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public void handleDataChange() {
            eventDispatcher.schedule(Master.this::onMasterChange, 0, TimeUnit.MILLISECONDS);
        }
    }

    public CompletableFuture<Void> schedule(Runnable task, int delayMS){
        CompletableFuture<Void> future = new CompletableFuture<>();

        eventDispatcher.schedule(() -> {
            innerRun(task, future);

        }, delayMS, TimeUnit.MILLISECONDS);

        return future;
    }

    private static void innerRun(Runnable task, CompletableFuture<Void> future){
        try{
            task.run();
            future.complete(null);
        }catch (Throwable th){
            LOG.warn("task:{} execute failed for exception:{}", task.toString(), th.toString());
            future.completeExceptionally(th);
        }

    }

    public CompletableFuture<Void> scheduleAtFixedRate(Runnable task, int periodMS){
        CompletableFuture<Void> future = new CompletableFuture<>();

        eventDispatcher.scheduleAtFixedRate(() -> {

            innerRun(task, future);
        },100, periodMS, TimeUnit.MILLISECONDS);

        return future;
    }


    public <T> CompletableFuture<T> failAfter(int durationMS) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        schedule(() -> { final TimeoutException ex = new TimeoutException("Timeout after " + durationMS); promise.completeExceptionally(ex); }, durationMS);
        return promise;
    }

    public <T> CompletableFuture<T> within(CompletableFuture<T> future, int durationMS) { final CompletableFuture<T> timeout = failAfter(durationMS); return future.applyToEither(timeout, Function.identity()); }


}
