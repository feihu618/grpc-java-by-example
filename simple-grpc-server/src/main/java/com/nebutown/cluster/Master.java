package com.nebutown.cluster;

public class Master {
    private DukerZKClient zkClient;
    private Node.NodeInfo nodeInfo;

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
                zkClient.registerBroker(nodeInfo);

            }
        });
    }

    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    public void elect() {

    }

    public void reElect() {

    }

    void onElectSuccess() {

    }

    void onElectFail() {

    }





}
