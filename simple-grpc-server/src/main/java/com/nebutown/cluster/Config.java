package com.nebutown.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

public class Config {

    static {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.yaml");
        try {
            INSTANCE = mapper.readValue(inputStream, Config.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("--- mapper.readValue failed", e);
        }
    }

    public static Config getInstance() {
        return INSTANCE;
    }

    private static final Config INSTANCE;

    private String clusterName;
    private Integer nodeId;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

}
