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
    static class MasterMovedException extends Exception{
        MasterMovedException(String message) {
            super(message);
        }
    }
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
    private int epochZkVersion;
    private Config config;

    public Master(DukerZKClient dukerZKClient, Config config) {
        this.eventDispatcher = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("Event-Dispatcher-%d").build());
        this.zkClient = dukerZKClient;
        this.config = config;
        this.nodeInfo = config.getNode();

        this.masterChangeHandler = new MasterChangeHandler();
        this.epochHandler = new EpochHandler();
        this.branchCommitHandler = new BranchCommitHandler();
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

                reElect();
            }
        });

        eventDispatcher.schedule(Master.this::onStartup, 0, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        zkClient.close();
        eventDispatcher.shutdown();
        this.masterChangeHandler = null;
        this.epochHandler = null;
        this.branchCommitHandler = null;
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
                triggerMasterMove();
        }
    }

    private void triggerMasterMove() {
        activeControllerId = -1;
        //TODO: clear
    }

    public boolean isMaster() {

        return activeControllerId == nodeInfo.getId();
    }

    public void reElect() {
        //TODO: clear
        elect();
    }

    void onStartup() {
        try {
            zkClient.registerZNodeChangeHandlerAndCheckExistence(masterChangeHandler);

            zkClient.registerZNodeChangeHandlerAndCheckExistence(epochHandler);
            zkClient.registerBroker(nodeInfo);

            elect();
        } catch (KeeperException e) {
            LOG.warn("--- onStartup happen KeeperException ", e);
        } catch (Throwable t) {

            LOG.warn("--- onStartup happen KeeperException ", t);
        }

    }

    void onElectSuccess() {
        try {

            readControllerEpochFromZooKeeper();

            incrementMasterEpoch();

        } catch (KeeperException e) {

            //TODO: if SessionExpire, ignore, others throw IllegalStateException
            LOG.warn("--- onElectSuccess happen KeeperException ", e);
        } catch (Throwable t) {
            //TODO:
            LOG.warn("--- onElectSuccess happen Throwable ", t);
        }
    }

    private void readControllerEpochFromZooKeeper() throws KeeperException {
        // initialize the controller epoch and zk version by reading from zookeeper
        Optional<Tuple<Integer, Stat>> epoch = zkClient.getMasterEpoch();
        epoch.ifPresent(tuple -> {

            Master.this.epoch = tuple.getS();
            Master.this.epochZkVersion = tuple.getT().getVersion();

            LOG.info("Initialized controller epoch to {} and zk version {}", tuple.getS(), tuple.getT().getVersion());
        });
    }

    public static void printDebugInfo() {

        System.out.println("Printing stack trace:");
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (int i = 2; i < elements.length; i++) {
            StackTraceElement s = elements[i];
            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
        }
    }

    private void incrementMasterEpoch() throws Exception {

        incrementMasterEpoch0();
        List<Integer> childrenIds = zkClient.getSortedBrokerList();
        createBranchAndCommit(childrenIds);

    }


    private void incrementMasterEpoch0() throws Exception{
        printDebugInfo();
        int newControllerEpoch = epoch + 1;
        ZKClient.SetDataResponse setDataResponse = zkClient.setMasterEpochRaw(newControllerEpoch, epochZkVersion);
        switch (setDataResponse.getResultCode()){
            case OK:
                epochZkVersion = setDataResponse.stat.getVersion();
                epoch = newControllerEpoch;
                break;
            case NONODE:
                // if path doesn't exist, this is the first master whose epoch should be 1
                // the following call can still fail if another master gets elected between checking if the path exists and
                // trying to create the master epoch path
                ZKClient.CreateResponse createResponse = zkClient.createMasterEpochRaw(1);
                switch (createResponse.getResultCode()){
                    case OK:
                         epoch = 1;
                         epochZkVersion = 1;
                         break;
                    case NODEEXISTS:
                        throw new MasterMovedException("Master moved to another node. Aborting controller startup procedure");
                    default:
                        KeeperException exception = createResponse.getResultException();
                        LOG.error("Error while incrementing controller epoch", exception);
                        throw exception;
            }
            default:
                throw new MasterMovedException("Master moved to another node. Aborting controller startup procedure");
        }
        LOG.info("Epoch incremented to {}", epoch);
    }

    private void createBranchAndCommit(List<Integer> childrenIds) throws KeeperException {

        zkClient.createBranch(epoch, childrenIds, System.currentTimeMillis());

        zkClient.updateBranch(epoch, nodeInfo.getId());

        //poll count of children
        ballotCountFuture = scheduleAtFixedRate(()->{

            int checkedSize = 0;
            try {
                checkedSize = zkClient.getChildren(ZKNode.BranchesZNode.getBallotPath(epoch)).size();

                LOG.info("--- checkedSize:{}", checkedSize);
            } catch (KeeperException e) {
                LOG.warn("--- getChildren on createBranchAndCommit happen exception", e);
            }
            if (childrenIds.size() == checkedSize){

                try {

                    zkClient.commitBranch(epoch);
                    if (ballotCountFuture != null)
                        ballotCountFuture.complete(null);
                } catch (KeeperException e) {
//                    e.printStackTrace();
                    LOG.warn("--- commitBranch happen exception:", e);
                }
            }

        }, 200);

        within(ballotCountFuture, 1000*30).handle((v, th) -> {

            if (th instanceof CompletionException) {

                eventDispatcher.schedule(Master.this::onIncrementMasterEpochFailed, 50, TimeUnit.MILLISECONDS);
            }

            return null;
        });

    }

    void onIncrementMasterEpochFailed() {
        try {

            incrementMasterEpoch();
        } catch (Exception e) {

            LOG.warn("--- incrementMasterEpoch happen exception:", e);
        }
    }

    void onElectFail() {

        try {

            readControllerEpochFromZooKeeper();
            zkClient.registerZNodeChangeHandler(branchCommitHandler);//
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void onMasterChange() {
        boolean wasMasterBeforeChange = isMaster();
        try {

            zkClient.registerZNodeChangeHandlerAndCheckExistence(masterChangeHandler);
            activeControllerId = zkClient.getMasterId().orElse(-1);
            if (wasMasterBeforeChange && !isMaster()) {
                onMasterResignation();
            }
        } catch (KeeperException e) {
            LOG.error("happen error when register handler:", e);
        }

    }

    void onNodeChange() {

        //TODO: performance
        try {

            incrementMasterEpoch();
        } catch (Exception e) {
            LOG.error("--- onNodeChange happen error when register handler:", e);
        }
    }

    void onViewChanged() {

        try {

            readControllerEpochFromZooKeeper();
            Cluster.getInstance().pause();
        } catch (KeeperException e) {//if this node can't connect to cluster, frontend proxy should not pass any request to this node

            LOG.error("--- onViewChanged happen error", e);
        }


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
        //TODO: unregister handler and close schedule for master
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


    public CompletableFuture<Void> scheduleAtFixedRate(Runnable task, int periodMS){
        CompletableFuture<Void> future = new CompletableFuture<>();

        final ScheduledFuture<?> scheduledFuture = eventDispatcher.scheduleAtFixedRate(task::run, 100, periodMS, TimeUnit.MILLISECONDS);

        future.handle((v, t) -> {
            scheduledFuture.cancel(false);
            return null;
        });

        return future;
    }


    public <T> CompletableFuture<T> failAfter(int durationMS) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        eventDispatcher.schedule(() -> { final TimeoutException ex = new TimeoutException("Timeout after " + durationMS); promise.completeExceptionally(ex); }, durationMS, TimeUnit.MILLISECONDS);
        return promise;
    }

    public <T> CompletableFuture<T> within(CompletableFuture<T> future, int durationMS) { final CompletableFuture<T> timeout = failAfter(durationMS); return future.applyToEither(timeout, Function.identity()); }


}
