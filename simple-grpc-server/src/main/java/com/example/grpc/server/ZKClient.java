package com.example.grpc.server;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
    private final String connectString;
    private final Integer sessionTimeoutMs;
    private final ZooKeeperClientWatcher zooKeeperClientWatcher;

    private volatile ZooKeeper zooKeeper;
    private ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();
    private ReentrantLock isConnectedOrExpiredLock = new ReentrantLock();
    private ConcurrentHashMap<String, ZNodeChangeHandler> zNodeChangeHandlers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ZNodeChildChangeHandler> zNodeChildChangeHandlers = new ConcurrentHashMap<>();

    public ZKClient(String connectString1, Integer sessionTimeoutMs1, ZooKeeperClientWatcher zooKeeperClientWatcher1) {
        this.connectString = connectString1;
        this.sessionTimeoutMs = sessionTimeoutMs1;
        this.zooKeeperClientWatcher = zooKeeperClientWatcher1;

        try {
            zooKeeper = new ZooKeeper(connectString1, sessionTimeoutMs1, zooKeeperClientWatcher1);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("--- create ZooKeeper failed");
        }
    }

    public void waitUntilConnected(Long timeout, TimeUnit timeUnit){

        throw new UnsupportedOperationException();
    }

    public ZooKeeper.States connectionState(){

        return zooKeeper.getState();
    }


    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     *
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
     * with either a GetDataRequest or ExistsRequest.
     *
     * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
     *
     * @param zNodeChangeHandler the handler to register
     */
    public void registerZNodeChangeHandler(ZNodeChangeHandler zNodeChangeHandler) {
        zNodeChangeHandlers.put(zNodeChangeHandler.getPath(), zNodeChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     * @param path the path of the handler to unregister
     */
    public void unregisterZNodeChangeHandler(String path) {
        zNodeChangeHandlers.remove(path);
    }

    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     *
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
     *
     * @param zNodeChildChangeHandler the handler to register
     */
    public void registerZNodeChildChangeHandler(ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zNodeChildChangeHandlers.put(zNodeChildChangeHandler.getPath(), zNodeChildChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     * @param path the path of the handler to unregister
     */
    public void unregisterZNodeChildChangeHandler(String path) {
        zNodeChildChangeHandlers.remove(path);
    }


    public void close() throws InterruptedException {
        LOG.info("Closing.");
        ReentrantReadWriteLock.WriteLock writeLock = initializationLock.writeLock();

        writeLock.lock();

        try{

            zNodeChangeHandlers.clear();
            zNodeChildChangeHandlers.clear();
            zooKeeper.close();

        }finally {
            writeLock.unlock();
        }
        // Shutdown scheduler outside of lock to avoid deadlock if scheduler
        // is waiting for lock to process session expiry
        expiryScheduler.shutdown();
        LOG.info("Closed.");
    }

    public Long sessionId(){

        ReentrantReadWriteLock.ReadLock readLock = initializationLock.readLock();
        readLock.lock();
        try{

            return zooKeeper.getSessionId();
        }finally {
            readLock.unlock();
        }
    }

    // Only for testing
    private ZooKeeper currentZooKeeper(){
        ReentrantReadWriteLock.ReadLock readLock = initializationLock.readLock();
        readLock.lock();
        try{

            return zooKeeper;
        }finally {
            readLock.unlock();
        }
    }

    private void reinitialize() throws InterruptedException {

        final ReentrantReadWriteLock.WriteLock writeLock = initializationLock.writeLock();

        writeLock.lock();
        try{
            if (!connectionState().isAlive()) {
                zooKeeper.close();
                LOG.info("Initializing a new session to $connectString.");
                // retry forever until ZooKeeper can be instantiated
                boolean connected = false;
                while (!connected) {
                    try {
                        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, zooKeeperClientWatcher);
                        connected = true;
                    } catch (Exception e){
                            LOG.info("Error when recreating ZooKeeper, retrying after a short sleep", e);
                            Thread.sleep(1000);
                    }
                }
            }
        }finally {
            writeLock.unlock();
        }

    }

    /**
     * Close the zookeeper client to force session reinitialization. This is visible for testing only.
     */
    public void forceReinitialize() throws InterruptedException {
        zooKeeper.close();
        reinitialize();
    }


    // Visibility for testing
    public void scheduleSessionExpiryHandler(){
        expiryScheduler.scheduleOnce("zk-session-expired", () => {
                LOG.info("Session expired.");
                reinitialize();
        });
    }

    private class ZooKeeperClientWatcher implements Watcher {
        @Override public void process(WatchedEvent event) {
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
            }else {

                switch (event.getType()) {
                    case final NodeChildrenChanged ZNodeChildChangeHandler zNodeChildChangeHandler = zNodeChildChangeHandlers.get(path);

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
}
