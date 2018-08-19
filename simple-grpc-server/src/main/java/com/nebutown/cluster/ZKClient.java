package com.nebutown.cluster;

import com.google.common.collect.Lists;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class ZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
    private final String connectString;
    private final Integer sessionTimeoutMs;
    private final ZooKeeperClientWatcher zooKeeperClientWatcher;

    private volatile ZooKeeper zooKeeper;
    private ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();
    private Semaphore inFlightRequests;
    private ReentrantLock isConnectedOrExpiredLock = new ReentrantLock();
    private Condition isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition();
    private ConcurrentHashMap<String, ZNodeChangeHandler> zNodeChangeHandlers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ZNodeChildChangeHandler> zNodeChildChangeHandlers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SessionStateChangeHandler> sessionStateChangeHandlers = new ConcurrentHashMap<>();
    private ScheduledExecutorService expiryScheduler = Executors.newSingleThreadScheduledExecutor();


    public ZKClient(String connectString1, Integer sessionTimeoutMs1, Integer maxInFlightRequests) {
        this.connectString = connectString1;
        this.sessionTimeoutMs = sessionTimeoutMs1;
        this.zooKeeperClientWatcher = new ZooKeeperClientWatcher();
        this.inFlightRequests = new Semaphore(maxInFlightRequests);
        try {
            zooKeeper = new ZooKeeper(connectString1, sessionTimeoutMs1, zooKeeperClientWatcher);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("--- create ZooKeeper failed");
        }
    }

    /**
     * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
     * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
     * @throws ZooKeeperClientExpiredException if the session expired either before or while waiting for connection.
     */
    public void waitUntilConnected() throws InterruptedException {
        isConnectedOrExpiredLock.lock();
        try{
            waitUntilConnected(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }finally {
            isConnectedOrExpiredLock.unlock();
        }
    }

    public void waitUntilConnected(Long timeout, TimeUnit timeUnit) throws InterruptedException {

        LOG.info("Waiting until connected.");
        long nanos = timeUnit.toNanos(timeout);
        isConnectedOrExpiredLock.lock();
        try {
            ZooKeeper.States state = connectionState();
            while (!state.isConnected() && state.isAlive()) {
                if (nanos <= 0) {
                    throw new ZooKeeperClientTimeoutException("Timed out waiting for connection while in state: "+state);
                }
                nanos = isConnectedOrExpiredCondition.awaitNanos(nanos);
                state = connectionState();
            }
            if (state == ZooKeeper.States.AUTH_FAILED) {
                throw new ZooKeeperClientAuthFailedException("Auth failed either before or while waiting for connection");
            } else if (state == ZooKeeper.States.CLOSED) {
                throw new ZooKeeperClientExpiredException("Session expired either before or while waiting for connection");
            }
        }finally {
            isConnectedOrExpiredLock.unlock();
        }

        LOG.info("Connected.");
    }

    public ZooKeeper.States connectionState() {

        return zooKeeper.getState();
    }

    public long getSessionId(){

        return zooKeeper.getSessionId();
    }


    /**
     * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
     *
     * @param request a single request to send and wait on.
     * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).
     */
    public AsyncResponse handleRequest(AsyncRequest request) throws InterruptedException {
        return handleRequests(Lists.newArrayList(request)).get(0);
    }

    /**
     * Send a pipelined sequence of requests and wait for all of their responses.
     *
     * The watch flag on each outgoing request will be set if we've already registered a handler for the
     * path associated with the request.
     *
     * @param requests a sequence of requests to send and wait on.
     * @return the responses for the requests. If all requests have the same type, the responses will have the respective
     * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
     * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
     */
    public List<AsyncResponse> handleRequests(List<AsyncRequest> requests) throws InterruptedException {
        if (requests.isEmpty())
            return Collections.emptyList();
        else {
            CountDownLatch countDownLatch = new CountDownLatch(requests.size());
            ArrayBlockingQueue<AsyncResponse> responseQueue = new ArrayBlockingQueue<>(requests.size());

            ReentrantReadWriteLock.ReadLock readLock = null;
            for (AsyncRequest request : requests) {

                inFlightRequests.acquire();
                try {
                        readLock = initializationLock.readLock();
                        readLock.lock();
                        send(request, asyncResponse -> {

                            responseQueue.add(asyncResponse);
                            inFlightRequests.release();
                            countDownLatch.countDown();
                        });

                    readLock.unlock();

                    }catch(Throwable e) {

                    inFlightRequests.release();
                    readLock.unlock();
                    throw e;
                }
            }
                countDownLatch.await();
            return Lists.newArrayList(responseQueue.iterator());
            }

     }

     @SuppressWarnings("all")
    private void send(AsyncRequest request, Consumer<AsyncResponse> processResponse) {

        long sendTimeMs = System.currentTimeMillis();
        switchType(request,
                            caze(ExistsRequest.class, existsRequest -> {
                                zooKeeper.exists(existsRequest.getPath(), shouldWatch(request), new AsyncCallback.StatCallback(){
                                    @Override
                                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                                        processResponse.accept(new ExistsResponse(Code.get(rc), path, ctx, stat, responseMetadata(sendTimeMs)));
                                    }}, existsRequest.getCtx());
                            }),
                            caze(GetDataRequest.class, getDataRequest -> {
                            zooKeeper.getData(getDataRequest.getPath(), shouldWatch(request), new AsyncCallback.DataCallback() {

                                @Override
                                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
                                    processResponse.accept(new GetDataResponse(Code.get(rc), path, ctx, data, stat, responseMetadata(sendTimeMs)));
                                    }}, getDataRequest.getCtx());
                            }),
                            caze(GetChildrenRequest.class, getChildrenRequest -> {
                            zooKeeper.getChildren(getChildrenRequest.getPath(), shouldWatch(request), new AsyncCallback.Children2Callback(){

                                @Override
                                public void processResult(int rc, String path, Object ctx, java.util.List<String> children, Stat stat){
                                    processResponse.accept(new GetChildrenResponse(Code.get(rc), path, ctx,
                                                Optional.ofNullable(children).orElseGet(Collections::emptyList), stat, responseMetadata(sendTimeMs)));
                                }}, getChildrenRequest.getCtx());
                            }),
                            caze(CreateRequest.class, createRequest -> {
                            zooKeeper.create(createRequest.getPath(), createRequest.getData(), createRequest.getAcl(), createRequest.getCreateMode(), new AsyncCallback.StringCallback(){

                                @Override
                                public void processResult(int rc, String path, Object ctx, String name){
                                    processResponse.accept(new CreateResponse(Code.get(rc), path, ctx, name, responseMetadata(sendTimeMs)));
                                }}, createRequest.getCtx());
                            }),
                            caze(SetDataRequest.class, setDataRequest -> {
                            zooKeeper.setData(setDataRequest.getPath(), setDataRequest.getData(), setDataRequest.getVersion(), new AsyncCallback.StatCallback(){

                                    @Override
                                    public void processResult(int rc, String path, Object ctx, Stat stat){
                                        processResponse.accept(new SetDataResponse(Code.get(rc), path, ctx, stat, responseMetadata(sendTimeMs)));
                                }}, setDataRequest.getCtx());
                            }),
                            caze(DeleteRequest.class, deleteRequest -> {
                            zooKeeper.delete(deleteRequest.getPath(), deleteRequest.getVersion(), new AsyncCallback.VoidCallback() {

                                @Override
                                public void processResult(int rc, String path, Object ctx){
                                    processResponse.accept(new DeleteResponse(Code.get(rc), path, ctx, responseMetadata(sendTimeMs)));
                                }}, deleteRequest.getCtx());
                            }),
                            caze(GetAclRequest.class, getAclRequest -> {
                            zooKeeper.getACL(getAclRequest.getPath(), null, new AsyncCallback.ACLCallback() {

                                @Override
                                public void processResult(int rc, String path, Object ctx, java.util.List<ACL> acl, Stat stat){
                                    processResponse.accept(new GetAclResponse(Code.get(rc), path, ctx, Optional.ofNullable(acl).orElseGet(Collections::emptyList), stat, responseMetadata(sendTimeMs)));
                                }}, getAclRequest.getCtx());
                            }),
                            caze(SetAclRequest.class, setAclRequest -> {
                            zooKeeper.setACL(setAclRequest.getPath(), setAclRequest.getAcl(), setAclRequest.getVersion(), new AsyncCallback.StatCallback() {

                                @Override
                                public void processResult(int rc, String path, Object ctx, Stat stat){
                                    processResponse.accept(new SetAclResponse(Code.get(rc), path, ctx, stat, responseMetadata(sendTimeMs)));
                            }}, setAclRequest.getCtx());
                            })
                            );
    }

    // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
    // may need to be updated.
    private Boolean shouldWatch(AsyncRequest request) {
        if (request instanceof GetChildrenRequest) {

            return zNodeChildChangeHandlers.keySet().contains(request.getPath());
        }else if (request instanceof ExistsRequest || request instanceof GetDataRequest){

            return zNodeChangeHandlers.keySet().contains(request.getPath());
        }else
            throw new IllegalArgumentException("That request:"+request+" is not watchable");
    }

    private ResponseMetadata responseMetadata(Long sendTimeMs){

        return new ResponseMetadata(sendTimeMs, System.currentTimeMillis());
    }


    public static <T> void switchType(T o, Consumer... a) {
        for (Consumer consumer : a)
            consumer.accept(o);
    }

    public static <T> Consumer caze(Class<T> cls, Consumer<T> c) {
        return obj -> Optional.of(obj).filter(cls::isInstance).map(cls::cast).ifPresent(c);
    }


    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     * <p>
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
     * with either a GetDataRequest or ExistsRequest.
     * <p>
     * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
     *
     * @param zNodeChangeHandler the handler to register
     */
    public void registerZNodeChangeHandler(ZNodeChangeHandler zNodeChangeHandler) {
        zNodeChangeHandlers.put(zNodeChangeHandler.getPath(), zNodeChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     *
     * @param path the path of the handler to unregister
     */
    public void unregisterZNodeChangeHandler(String path) {
        zNodeChangeHandlers.remove(path);
    }

    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     * <p>
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
     *
     * @param zNodeChildChangeHandler the handler to register
     */
    public void registerZNodeChildChangeHandler(ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zNodeChildChangeHandlers.put(zNodeChildChangeHandler.getPath(), zNodeChildChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     *
     * @param path the path of the handler to unregister
     */
    public void unregisterZNodeChildChangeHandler(String path) {
        zNodeChildChangeHandlers.remove(path);
    }


    public void registerSessionStateChangeHandler(SessionStateChangeHandler sessionStateChangeHandler) {

        sessionStateChangeHandlers.put(sessionStateChangeHandler.getName(), sessionStateChangeHandler);
    }

    public void unregisterSessionStateChangeHandler(String name) {

        sessionStateChangeHandlers.remove(name);
    }

    public void close() throws InterruptedException {
        LOG.info("Closing.");
        ReentrantReadWriteLock.WriteLock writeLock = initializationLock.writeLock();

        writeLock.lock();

        try {

            zNodeChangeHandlers.clear();
            zNodeChildChangeHandlers.clear();
            zooKeeper.close();

        } finally {
            writeLock.unlock();
        }
        // Shutdown scheduler outside of lock to avoid deadlock if scheduler
        // is waiting for lock to process session expiry
        expiryScheduler.shutdown();
        LOG.info("Closed.");
    }

    public Long sessionId() {

        ReentrantReadWriteLock.ReadLock readLock = initializationLock.readLock();
        readLock.lock();
        try {

            return zooKeeper.getSessionId();
        } finally {
            readLock.unlock();
        }
    }

    // Only for testing
    private ZooKeeper currentZooKeeper() {
        ReentrantReadWriteLock.ReadLock readLock = initializationLock.readLock();
        readLock.lock();
        try {

            return zooKeeper;
        } finally {
            readLock.unlock();
        }
    }

    private void reinitialize() throws InterruptedException {

        final Collection<SessionStateChangeHandler> values = sessionStateChangeHandlers.values();
        values.parallelStream()
                .forEach(SessionStateChangeHandler::beforeInitializingSession);

        final ReentrantReadWriteLock.WriteLock writeLock = initializationLock.writeLock();

        writeLock.lock();
        try {
            if (!connectionState().isAlive()) {
                zooKeeper.close();
                LOG.info("Initializing a new session to $connectString.");
                // retry forever until ZooKeeper can be instantiated
                boolean connected = false;
                while (!connected) {
                    try {
                        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, zooKeeperClientWatcher);
                        connected = true;
                    } catch (Exception e) {
                        LOG.info("Error when recreating ZooKeeper, retrying after a short sleep", e);
                        Thread.sleep(1000);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }

        values.parallelStream()
                .forEach(SessionStateChangeHandler::afterInitializingSession);

    }

    /**
     * Close the zookeeper client to force session reinitialization. This is visible for testing only.
     */
    public void forceReinitialize() throws InterruptedException {
        zooKeeper.close();
        reinitialize();
    }


    // Visibility for testing
    public void scheduleSessionExpiryHandler() {
        expiryScheduler.schedule(() -> {

            LOG.info("Session expired.");
            try {
                reinitialize();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 100, TimeUnit.MILLISECONDS);

    }

    private class ZooKeeperClientWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            LOG.debug("Received event: {}", event);

            final String path = event.getPath();
            if (path == null) {
                final Event.KeeperState state = event.getState();
                if (state == Event.KeeperState.AuthFailed) {
                    LOG.error("Auth failed.");
                    throw new IllegalStateException("--- Auth failed");
                } else if (state == Event.KeeperState.Expired) {
                    scheduleSessionExpiryHandler();
                }
            } else {

                switch (event.getType()) {
                    case NodeChildrenChanged:
                        final ZNodeChildChangeHandler zNodeChildChangeHandler = zNodeChildChangeHandlers.get(path);

                        Optional.ofNullable(zNodeChildChangeHandler).ifPresent(ZNodeChildChangeHandler::handleChildChange);
                        break;
                    case NodeCreated:
                        Optional.ofNullable(zNodeChangeHandlers.get(path)).ifPresent(ZNodeChangeHandler::handleCreation);
                        break;
                    case NodeDeleted:
                        Optional.ofNullable(zNodeChangeHandlers.get(path)).ifPresent(ZNodeChangeHandler::handleDeletion);
                        break;
                    case NodeDataChanged:
                        Optional.ofNullable(zNodeChangeHandlers.get(path)).ifPresent(ZNodeChangeHandler::handleDataChange);
                        break;

                }

            }
        }
    }

    interface ZNodeChangeHandler {
        String getPath();

        void handleCreation();

        void handleDeletion();

        void handleDataChange();
    }

    interface ZNodeChildChangeHandler {
        String getPath();

        void handleChildChange();
    }

    interface SessionStateChangeHandler {
        String getName();

        void beforeInitializingSession();
        void afterInitializingSession();
    }


    interface AsyncRequest {
        /**
         * This type member allows us to define methods that take requests and return responses with the correct types.
         * See ``ZooKeeperClient.handleRequests`` for example.
         */
//        type Response <: AsyncResponse
        public String getPath();

        public Optional<Object> getCtx();
    }

    public static class CreateRequest implements AsyncRequest {
        String path;
        byte[] data;
        List<ACL> acl;
        CreateMode createMode;
        Object ctx;

        public CreateRequest(String path, byte[] data, List<ACL> acl, CreateMode createMode, Object ctx) {
            this.path = path;
            this.data = data;
            this.acl = acl;
            this.createMode = createMode;
            this.ctx = ctx;
        }



        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }

        public byte[] getData() {
            return data;
        }

        public List<ACL> getAcl() {
            return acl;
        }

        public CreateMode getCreateMode() {
            return createMode;
        }
    }

    public static class DeleteRequest implements AsyncRequest {
        String path; Integer version; Object ctx;

        public DeleteRequest(String path, Integer version, Object ctx) {
            this.path = path;
            this.version = version;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }

        public int getVersion() {
            return version;
        }
    }

    public static class ExistsRequest implements AsyncRequest {
        String path;
        Object ctx;

        public ExistsRequest(String path, Object ctx) {
            this.path = path;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }
    }

    public static class GetDataRequest implements AsyncRequest {
        String path;
        Object ctx;

        public GetDataRequest(String path, Object ctx) {
            this.path = path;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }
    }

    public static class SetDataRequest implements AsyncRequest {
        String path; byte[] data; Integer version; Object ctx;

        public SetDataRequest(String path, byte[] data, Integer version, Object ctx) {
            this.path = path;
            this.data = data;
            this.version = version;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }

        public byte[] getData() {
            return data;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class GetAclRequest implements AsyncRequest {
        String path;
        Object ctx;

        public GetAclRequest(String path, Object ctx) {
            this.path = path;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }
    }

    public static class SetAclRequest implements AsyncRequest {
        String path; List<ACL> acl; Integer version; Object ctx;

        public SetAclRequest(String path, List<ACL> acl, Integer version, Object ctx) {
            this.path = path;
            this.acl = acl;
            this.version = version;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public java.util.Optional<Object> getCtx() {
            return java.util.Optional.ofNullable(ctx);
        }

        public List<ACL> getAcl() {
            return acl;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class GetChildrenRequest implements AsyncRequest {
        String path; Object ctx;

        public GetChildrenRequest(String path, Object ctx) {
            this.path = path;
            this.ctx = ctx;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }
    }

    interface AsyncResponse {
        public Code getResultCode();

        public String getPath();

        public Optional<Object> getCtx();

        /**
         * Return None if the result code is OK and KeeperException otherwise.
         */
        public default KeeperException getResultException() {

            return getResultCode() == Code.OK ? null : KeeperException.create(getResultCode(), getPath());
        }

        public default void maybeThrow() throws KeeperException {
            if (Code.OK != getResultCode())
                throw KeeperException.create(getResultCode(), getPath());
        }


        public ResponseMetadata getMetadata();
    }

    public static class ResponseMetadata {
        private final Long sendTimeMs;
        private final Long receivedTimeMs;

        public ResponseMetadata(Long sendTimeMs, Long receivedTimeMs) {

            this.sendTimeMs = sendTimeMs;
            this.receivedTimeMs = receivedTimeMs;
        }

        public Long responseTimeMs() {

            return receivedTimeMs - sendTimeMs;
        }
    }

    public static class CreateResponse implements AsyncResponse {
        KeeperException.Code resultCode; String path; Object ctx; String name; ResponseMetadata metadata;

        public CreateResponse(Code resultCode, String path, Object ctx, String name, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.name = name;
            this.metadata = metadata;
        }

        public String getName() {

            return name;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ZKClient.ResponseMetadata getMetadata() {
            return metadata;
        }
    }

    public static class DeleteResponse implements AsyncResponse {

        Code resultCode; String path; Object ctx; ResponseMetadata metadata;

        public DeleteResponse(Code resultCode, String path, Object ctx, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }
    }

    public static class ExistsResponse implements AsyncResponse {
        Code resultCode; String path; Object ctx; Stat stat; ResponseMetadata metadata;

        public ExistsResponse(Code resultCode, String path, Object ctx, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }
    }

    public static class GetDataResponse implements AsyncResponse {
        Code resultCode; String path; Object ctx; byte[] data; org.apache.zookeeper.data.Stat stat; ResponseMetadata metadata;

        public GetDataResponse(Code resultCode, String path, Object ctx, byte[] data, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.data = data;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }

        public Stat getStat(){

            return stat;
        }

        public byte[] getData() {
            return data;
        }
    }

    public static class SetDataResponse implements AsyncResponse {
        Code resultCode; String path; Object ctx; Stat stat; ResponseMetadata metadata;

        public SetDataResponse(Code resultCode, String path, Object ctx, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }

        public Stat getStat() {
            return stat;
        }
    }

    public static class GetAclResponse implements AsyncResponse {
        Code resultCode; String path; Object ctx; List<ACL> acl; Stat stat;
        ResponseMetadata metadata;

        public GetAclResponse(Code resultCode, String path, Object ctx, List<ACL> acl, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.acl = acl;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }

        public List<ACL> getAcl() {
            return acl;
        }

        public Stat getStat() {
            return stat;
        }
    }

    public static class SetAclResponse implements AsyncResponse {
        Code resultCode; String path; Object ctx; Stat stat; ResponseMetadata metadata;

        public SetAclResponse(Code resultCode, String path, Object ctx, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public java.lang.String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }
    }

    public static class GetChildrenResponse implements AsyncResponse {

        Code resultCode; String path; Object ctx; List<String> children; Stat stat;
        ResponseMetadata metadata;

        public GetChildrenResponse(Code resultCode, String path, Object ctx, List<String> children, Stat stat, ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.children = children;
            this.stat = stat;
            this.metadata = metadata;
        }

        @Override
        public Code getResultCode() {
            return resultCode;
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public Optional<Object> getCtx() {
            return Optional.ofNullable(ctx);
        }

        @Override
        public ResponseMetadata getMetadata() {
            return metadata;
        }

        public List<String> getChildren() {
            return children;
        }

        public Stat getStat() {
            return stat;
        }
    }

    public class ZooKeeperClientException extends RuntimeException {
        public ZooKeeperClientException(String message) {
            super(message);
        }
    }

    public class ZooKeeperClientExpiredException extends ZooKeeperClientException {
        public ZooKeeperClientExpiredException(String message) {
            super(message);
        }
    }

    public class ZooKeeperClientAuthFailedException extends ZooKeeperClientException {
        public ZooKeeperClientAuthFailedException(String message) {
            super(message);
        }
    }

    public class ZooKeeperClientTimeoutException extends ZooKeeperClientException {
        ZooKeeperClientTimeoutException(String message) {
            super(message);
        }
    }

}
