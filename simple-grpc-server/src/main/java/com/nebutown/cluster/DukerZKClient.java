package com.nebutown.cluster;

import com.google.common.collect.Lists;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.KeeperException.Code.OK;


public class DukerZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(DukerZKClient.class);
    private static final int MatchAnyVersion = -1; // if used in a conditional set, matches any version (the value should match ZooKeeper codebase)
    private static final int UnknownVersion = -2;  // Version returned from get if node does not exist (internal constant for Kafka codebase, unused value in ZK)

    private static final Stat  NoStat = new Stat();
    private static final String NODES_SEQ_ID = "nodes/seqid";
    private static final String CLUSTER_ID = "cluster/id";


    private ZKClient zkClient;
    private boolean isSecure;
    private List<String> sensitiveRootPaths = Collections.emptyList();

    public DukerZKClient() { }
    

    /**
     * Create a sequential persistent path. That is, the znode will not be automatically deleted upon client's disconnect
     * and a monotonically increasing number will be appended to its name.
     *
     * @param path the path to create (with the monotonically increasing number appended)
     * @param data the znode data
     * @return the created path (including the appended monotonically increasing number)
     */
    public String createSequentialPersistentPath(String path, byte[] data) {

        ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(path, data, acls(path), CreateMode.PERSISTENT_SEQUENTIAL, null);
        ZKClient.CreateResponse createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);

        return createResponse.getName();
    }

    public void registerBroker(Node.BrokerInfo brokerInfo) {
        String path = brokerInfo.path();


        try {
            checkedEphemeralCreate(path, brokerInfo.toJsonBytes());
            LOG.info("Registered broker {} at path {} with addresses: ${brokerInfo.broker.endPoints}", brokerInfo.getBroker().getId(), path);
        } catch (KeeperException e) {

            LOG.error("--- registerBroker:{} failed", brokerInfo, e);
            throw new RuntimeException("registerBroker failed, please check configuration~");
        }

    }


    /**
     * Registers a given broker in zookeeper as the controller.
     * @param controllerId the id of the broker that is to be registered as the controller.
     * @param timestamp the timestamp of the controller election.
     * @throws KeeperException if an error is returned by ZooKeeper.
     */
    public void registerController(Integer controllerId, Long timestamp) {
        String path = Cluster.MasterZNode.path();
        try {
            checkedEphemeralCreate(path, Cluster.MasterZNode.encode(controllerId, timestamp));
        } catch (KeeperException e) {
            e.printStackTrace();//
            LOG.info("--- The {} registerController falied", controllerId);
        }
    }

    public void updateBrokerInfo(Node.BrokerInfo brokerInfo) {
        throw new UnsupportedOperationException();
    }
    
    

    /**
     * Sets the controller epoch conditioned on the given epochZkVersion.
     * @param epoch the epoch to set
     * @param epochZkVersion the expected version number of the epoch znode.
     * @return SetDataResponse
     */
    public ZKClient.SetDataResponse setControllerEpochRaw(Integer epoch, Integer epochZkVersion) {
        ZKClient.SetDataRequest setDataRequest = new ZKClient.SetDataRequest(Cluster.EpochZNode.path(), Cluster.EpochZNode.encode(epoch), epochZkVersion, null);
        return (ZKClient.SetDataResponse) retryRequestUntilConnected(setDataRequest);
    }

    /**
     * Creates the controller epoch znode.
     * @param epoch the epoch to set
     * @return CreateResponse
     */
    public ZKClient.CreateResponse createControllerEpochRaw(Integer epoch) {
        ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(Cluster.EpochZNode.path(), Cluster.EpochZNode.encode(epoch),
                acls(Cluster.EpochZNode.path()), CreateMode.PERSISTENT, null);
        return (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);
    }
    

    /**
     * Gets all brokers in the cluster.
     * @return sequence of brokers in the cluster.
     */
    public List<Node> getAllBrokersInCluster() throws KeeperException {

        ArrayList<Integer> brokerIds = getSortedBrokerList();
        ArrayList<ZKClient.GetDataRequest> getDataRequests = brokerIds.stream().map(brokerId -> new ZKClient.GetDataRequest(Node.ZNode.getPath(brokerId), brokerId)).collect(Collectors.toCollection(ArrayList::new));
        ArrayList<ZKClient.AsyncResponse> getDataResponses = retryRequestsUntilConnected(getDataRequests);

        final List<Node> nodeList = getDataResponses.stream().map(getDataResponse -> {
            Integer brokerId = (Integer) getDataResponse.getCtx().get();
            switch (getDataResponse.getResultCode()) {
                case OK:
                    return Node.ZNode.decode(brokerId, ((ZKClient.GetDataResponse) getDataResponse).getData()).getBroker();
                case NONODE:
                default:
                    return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        if(nodeList.size() == getDataRequests.size())
            return nodeList;
        else
            throw new IllegalStateException("--- try next");
    }

    /**
     * Get a broker from ZK
     * @return an optional Broker
     */
    public Optional<Node> getBroker(Integer brokerId) throws KeeperException {
        ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(Node.ZNode.getPath(brokerId), null);
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);
        switch (getDataResponse.getResultCode()){
            case OK:
                return Optional.ofNullable(Node.ZNode.decode(brokerId, getDataResponse.data)).map(Node.BrokerInfo::getBroker);
            case NONODE:
                return Optional.empty();
            default:
                throw getDataResponse.getResultException();
        }
    }

    /**
     * Gets the list of sorted broker Ids
     */
    public ArrayList<Integer> getSortedBrokerList() throws KeeperException {

        return getChildren(Node.ZNode.getPath()).stream().mapToInt(Integer::parseInt).sorted().boxed().collect(Collectors.toCollection(ArrayList::new));

    }

    

    /**
     * Gets the data and version at the given zk path
     * @param path zk node path
     * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
     *         and second element is zk node version.
     *         returns (None, ZkVersion.UnknownVersion) if node doesn't exist and throws exception for any error
     */
    public Optional<Tuple<byte[], Integer>> getDataAndVersion(String path) throws KeeperException {

        Optional<Tuple<byte[], Stat>> optional = getDataAndStat(path);

        return optional.map(tuple -> {

            if (Objects.equals(tuple.getT(), NoStat))
                return Tuple.of(tuple.getS(), UnknownVersion);
            else
                return Tuple.of(tuple.getS(), tuple.getT().getVersion());

        });
    }

    /**
     * Gets the data and Stat at the given zk path
     * @param path zk node path
     * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
     *         and second element is zk node stats.
     *         returns (None, ZkStat.NoStat) if node doesn't exists and throws exception for any error
     */
    public Optional<Tuple<byte[], Stat>> getDataAndStat(String path) throws KeeperException {
        ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(path, null);
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);

        switch (getDataResponse.getResultCode()){
            case OK:
                return Optional.of(Tuple.of(getDataResponse.getData(), getDataResponse.getStat()));
            case NONODE:
                return Optional.of(Tuple.of(null, NoStat));
            default:
                throw getDataResponse.getResultException();
        }
    }

    /**
     * Gets all the child nodes at a given zk node path
     * @param path
     * @return list of child node names
     */
    public List<String> getChildren(String path) throws KeeperException {

        ZKClient.GetChildrenResponse getChildrenResponse = (ZKClient.GetChildrenResponse) retryRequestUntilConnected(new ZKClient.GetChildrenRequest(path, null));
        switch (getChildrenResponse.getResultCode()){
            case OK:
                return getChildrenResponse.getChildren();
            case NONODE:
                return Collections.emptyList();
            default:
                throw getChildrenResponse.getResultException();
        }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
     * exist, the current version is not the expected version, etc.) return (false, ZkVersion.UnknownVersion)
     *
     * When there is a ConnectionLossException during the conditional update, ZookeeperClient will retry the update and may fail
     * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
     * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
     */
    public Tuple<Boolean,Integer> conditionalUpdatePath(String path, byte[] data, Integer expectVersion,
                              ThreeArgsFunction<DukerZKClient, String, byte[], Tuple<Boolean,Integer>> optionalChecker) {


        throw new UnsupportedOperationException();
    }
    


    /**
     * Gets the controller id.
     * @return optional integer that is Some if the controller znode exists and can be parsed and None otherwise.
     */
    public Optional<Integer> getControllerId() throws KeeperException {
        ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(Cluster.MasterZNode.path(), null);
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);
        switch (getDataResponse.getResultCode()){
            case OK:
                Cluster.MasterZNode.decode(getDataResponse.getData());
            case NONODE:
                return Optional.empty();
            default:
                throw getDataResponse.getResultException();
        }
    }

    /**
     * Deletes the controller znode.
     */
    public void deleteController() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the controller epoch.
     * @return optional (Integer, Stat) that is Some if the controller epoch path exists and None otherwise.
     */
    public Optional<Tuple<Integer, Stat>> getControllerEpoch() throws KeeperException {
        ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(Cluster.EpochZNode.path(), null);
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);
        switch(getDataResponse.getResultCode()) {
            case OK:
                int epoch = Cluster.EpochZNode.decode(getDataResponse.getData());
                return Optional.of(Tuple.of(epoch, getDataResponse.getStat()));
            case NONODE:
                return Optional.empty();
            default:
                throw getDataResponse.getResultException();
        }
    }


    /**
     * Deletes the zk node recursively
     * @param path
     * @return  return true if it succeeds, false otherwise
     */
    public Boolean deletePath(String path) {
        try {
            return deleteRecursive(path);
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        return false;
    }
    

    /**
     * This registers a ZNodeChangeHandler and attempts to register a watcher with an ExistsRequest, which allows data
     * watcher registrations on paths which might not even exist.
     *
     * @param zNodeChangeHandler
     * @return `true` if the path exists or `false` if it does not
     * @throws KeeperException if an error is returned by ZooKeeper
     */
    public Boolean registerZNodeChangeHandlerAndCheckExistence(ZKClient.ZNodeChangeHandler zNodeChangeHandler) throws KeeperException {
        zkClient.registerZNodeChangeHandler(zNodeChangeHandler);
        ZKClient.ExistsResponse existsResponse = (ZKClient.ExistsResponse) retryRequestUntilConnected(new ZKClient.ExistsRequest(zNodeChangeHandler.getPath(), null));
        switch (existsResponse.getResultCode()){
            case OK:
                return true;
            case NONODE:
                return false;
            default: throw existsResponse.getResultException();
        }
    }

    /**
     * See ZooKeeperClient.registerZNodeChangeHandler
     * @param zNodeChangeHandler
     */
    public void registerZNodeChangeHandler(ZKClient.ZNodeChangeHandler zNodeChangeHandler) {
        zkClient.registerZNodeChangeHandler(zNodeChangeHandler);
    }

    /**
     * See ZooKeeperClient.unregisterZNodeChangeHandler
     * @param path
     */
    public void unregisterZNodeChangeHandler(String path) {
        zkClient.unregisterZNodeChangeHandler(path);
    }

    /**
     * See ZooKeeperClient.registerZNodeChildChangeHandler
     * @param zNodeChildChangeHandler
     */
    public void registerZNodeChildChangeHandler(ZKClient.ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zkClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler);
    }

    /**
     * See ZooKeeperClient.unregisterZNodeChildChangeHandler
     * @param path
     */
    public void unregisterZNodeChildChangeHandler(String path) {
        zkClient.unregisterZNodeChildChangeHandler(path);
    }



    /**
     * Close the underlying ZooKeeperClient.
     */
    public void  close() {

        try {

            zkClient.close();
        } catch (InterruptedException e) {
//            e.printStackTrace();
            LOG.warn("--- happen InterruptedException");
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Get the cluster id.
     *
     * @return optional cluster id in String.
     */
    public Optional<String> getClusterId() throws KeeperException {
        ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(CLUSTER_ID, null);
        ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);
        switch (getDataResponse.getResultCode()){
            case OK:
                return Optional.ofNullable(Cluster.ClusterZNode.fromJson(getDataResponse.getData())).map(map -> map.get("id"));
            case NONODE:
                return Optional.empty();
            default: throw getDataResponse.getResultException();
        }
    }

    /**
     * Create the cluster Id. If the cluster id already exists, return the current cluster id.
     * @return  cluster id
     */
    public String createOrGetClusterId(String proposedClusterId) {
        try {
            createRecursive(CLUSTER_ID, Cluster.ClusterZNode.toJson(proposedClusterId), true);
            return proposedClusterId;
        } catch (Exception e){
            throw new RuntimeException("Failed to get cluster id from Zookeeper. This can happen if /cluster/id is deleted from Zookeeper.");
        }
    }

    /**
     * Generate a broker id by updating the broker sequence id path in ZK and return the version of the path.
     * The version is incremented by one on every update starting from 1.
     * @return sequence number as the broker id
     */
    public int generateBrokerSequenceId() throws KeeperException {
        ZKClient.SetDataRequest setDataRequest = new ZKClient.SetDataRequest(NODES_SEQ_ID, new byte[0], MatchAnyVersion, null);
        ZKClient.SetDataResponse setDataResponse = (ZKClient.SetDataResponse) retryRequestUntilConnected(setDataRequest);
        switch (setDataResponse.getResultCode()){
            case OK:
                return setDataResponse.getStat().getVersion();
            case NONODE:
                // maker sure the path exists
                createRecursive(NODES_SEQ_ID, new byte[0], false);
                generateBrokerSequenceId();

            default:
                throw setDataResponse.getResultException();
        }
    }



    /**
     * Make sure a persistent path exists in ZK.
     * @param path
     */
    public void makeSurePersistentPathExists(String path) throws KeeperException {

        createRecursive(path, null, false);
    }

    

    /**
     * Deletes the given zk path recursively
     * @param path
     * @return true if path gets deleted successfully, false if root path doesn't exist
     * @throws KeeperException if there is an error while deleting the znodes
     */
    public boolean deleteRecursive(String path) throws KeeperException {

        ZKClient.GetChildrenResponse getChildrenResponse = (ZKClient.GetChildrenResponse) retryRequestUntilConnected(new ZKClient.GetChildrenRequest(path, null));
        switch (getChildrenResponse.getResultCode()){
            case OK:
                getChildrenResponse.getChildren().forEach(child -> {
                    try {
                        deleteRecursive(path+"/"+child);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    }
                });
                ZKClient.DeleteResponse deleteResponse = (ZKClient.DeleteResponse) retryRequestUntilConnected(new ZKClient.DeleteRequest(path, MatchAnyVersion, null));
                if (deleteResponse.resultCode != OK && deleteResponse.resultCode != NONODE) {
                    throw deleteResponse.getResultException();
                }
                return true;
            case NONODE:
                return  false;

            default:
                throw getChildrenResponse.getResultException();
        }
    }

    public boolean pathExists(String path) throws InterruptedException, KeeperException {
        ZKClient.ExistsRequest existsRequest = new ZKClient.ExistsRequest(path, null);
        ZKClient.ExistsResponse existsResponse = (ZKClient.ExistsResponse) retryRequestUntilConnected(existsRequest);
        switch (existsResponse.getResultCode()){
            case OK:
                return true;
            case NONODE:
                return false;
            default:
                throw existsResponse.getResultException();
        }
       
    }



    public void createRecursive(String path, byte[] data, boolean throwIfPathExists) throws KeeperException {


        ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(path, data, acls(path), CreateMode.PERSISTENT, null);
        ZKClient.CreateResponse createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);

        if (throwIfPathExists && createResponse.getResultCode() == Code.NODEEXISTS) {
            throw KeeperException.create(createResponse.getResultCode(), createResponse.getPath());
        } else if (createResponse.resultCode == NONODE) {
            createRecursive0(parentPath(path));
            createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);
            if (throwIfPathExists || createResponse.resultCode != Code.NODEEXISTS)
                throw KeeperException.create(createResponse.getResultCode(), createResponse.getPath());
        } else if (createResponse.resultCode != Code.NODEEXISTS)
            throw KeeperException.create(createResponse.getResultCode(), createResponse.getPath());

    }


    private String parentPath(String path) {
        int indexOfLastSlash = path.lastIndexOf("/");
        if (indexOfLastSlash == -1) throw new IllegalArgumentException("Invalid path " + path);
        return path.substring(0, indexOfLastSlash);
    }

    private void createRecursive0(String path) throws KeeperException {
        ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(path, null, acls(path), CreateMode.PERSISTENT, null);
        ZKClient.CreateResponse createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);
        if (createResponse.getResultCode() == NONODE) {
            createRecursive0(parentPath(path));
            createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);
            if (createResponse.resultCode != OK && createResponse.resultCode != Code.NODEEXISTS) {
                throw createResponse.getResultException();
            }
        } else if (createResponse.resultCode != OK && createResponse.resultCode != Code.NODEEXISTS) {
            throw createResponse.getResultException();
        }
    }

    private ZKClient.AsyncResponse retryRequestUntilConnected(ZKClient.AsyncRequest request) {

        return retryRequestsUntilConnected(Lists.newArrayList(request)).get(0);
    }

    private List<ZKClient.AsyncResponse> handleRequests0(ArrayList<ZKClient.AsyncRequest> requests){

        try {
            return zkClient.handleRequests(requests);
        } catch (InterruptedException e) {
            LOG.warn("--- when exec handleRequests, happen exception", e);
            Thread.currentThread().interrupt();
        }

        return Collections.emptyList();
    }

    private ArrayList<ZKClient.AsyncResponse> retryRequestsUntilConnected(ArrayList<? extends ZKClient.AsyncRequest> requests) {
        ArrayList<ZKClient.AsyncRequest> remainingRequests = new ArrayList<>(requests);
        ArrayList<ZKClient.AsyncResponse> responses = new ArrayList<>();

        while (!remainingRequests.isEmpty()) {
            final List<ZKClient.AsyncResponse> batchResponses = handleRequests0(remainingRequests);

            // Only execute slow path if we find a response with CONNECTIONLOSS
            if (batchResponses.stream().anyMatch(asyncResponse -> Code.CONNECTIONLOSS.equals(asyncResponse.getResultCode()))) {

                remainingRequests.clear();

                IntStream.range(0, responses.size())
                        .forEach(i -> {

                            ZKClient.AsyncResponse response = batchResponses.get(i);
                            if (response.getResultCode() == Code.CONNECTIONLOSS)
                                remainingRequests.add(requests.get(i));
                            else
                                responses.add(response);
                        });


                if (!remainingRequests.isEmpty()) {
                    try {
                        zkClient.waitUntilConnected();
                    } catch (InterruptedException e) {
                        LOG.warn("--- when exec handleRequests, happen exception", e);
                        Thread.currentThread().interrupt();
                    }
                }
            } else {
                remainingRequests.clear();
                responses.addAll(batchResponses);
            }
        }
        return responses;
    }

    private void checkedEphemeralCreate(String path, byte[] data) throws KeeperException {
        CheckedEphemeral checkedEphemeral = new CheckedEphemeral(path, data);
        LOG.info("Creating {} (is it secure? {})",path, isSecure);
        KeeperException.Code code = checkedEphemeral.create();
        LOG.info("Result of znode creation at {} is: {}", path, code);
        if (code != OK)
            throw  KeeperException.create(code);
    }

    public Boolean sensitivePath(String path){
        return path != null && sensitiveRootPaths.contains(path);
    }


    private ArrayList<ACL> acls(String path){

        if (isSecure) {
            ArrayList<ACL> acls = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
            if (!sensitivePath(path))
                acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
            return acls;
        } else return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    private class CheckedEphemeral {

        private final String path;
        private final byte[] data;

        CheckedEphemeral(String path, byte[] data){
            this.path = path;
            this.data = data;
        }
        KeeperException.Code create() {

            ZKClient.CreateRequest createRequest = new ZKClient.CreateRequest(path, data, acls(path), CreateMode.EPHEMERAL, null);
            ZKClient.CreateResponse createResponse = (ZKClient.CreateResponse) retryRequestUntilConnected(createRequest);
             switch (createResponse.getResultCode()){
                 case OK:
                     return OK;
                 case NODEEXISTS:
                     return getAfterNodeExists();
                default:
                    LOG.error("Error while creating ephemeral at {} with return code: {}", path, createResponse.getResultCode());
                    return createResponse.getResultCode();
             }
        }

        private Code getAfterNodeExists() {
            ZKClient.GetDataRequest getDataRequest = new ZKClient.GetDataRequest(path, null);
            ZKClient.GetDataResponse getDataResponse = (ZKClient.GetDataResponse) retryRequestUntilConnected(getDataRequest);
            switch (getDataResponse.getResultCode()){
                case OK:
                    if (getDataResponse.getStat().getEphemeralOwner() != zkClient.getSessionId()) {
                        LOG.error("Error while creating ephemeral at {}, node already exists and owner " +
                                "{} does not match current session {}", path, getDataResponse.getStat().getEphemeralOwner(), zkClient.getSessionId());
                        return Code.NODEEXISTS;
                    }else
                        return OK;
                case NONODE:
                    LOG.info("The ephemeral node at {} went away while reading it, attempting create() again", path);
                    return create();

                default:
                    LOG.error("Error while creating ephemeral at {} as it already exists and error getting the node data due to {}", path, getDataResponse.getResultCode());
                    return getDataResponse.getResultCode();

      }
    }
    }
}
