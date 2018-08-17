package com.nebutown.cluster;

import com.example.grpc.server.RID;
import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 *
 * {
 *   "version":1,
 *   "host":"localhost",
 *   "port":9092,
 *   "jmx_port":9999,
 *   "timestamp":"2233345666",
 *   "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
 *   "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"},
 *   "rack":"dc1"
 * }
 */
public class Node {

    private int id;

    public int getId() {

        return id;
    }


    public String getRecord(RID request, UUID key) {
        throw new UnsupportedOperationException();
    }

    public <T> void putRecord(RID request, UUID key, T data) {
        throw new UnsupportedOperationException();
    }

    public boolean tryCommit(RID request) {
        throw new UnsupportedOperationException();
    }

    public void commit(RID request) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     *
     */
    public static class NodeInfo {

        private int id;
        private String host;
        private int port;
        private int jmxPort;
        private Map<String, String> option;
        private int version;

        public int getId() {

            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public Map<String, String> getOption() {
            return option;
        }

        public void setOption(Map<String, String> option) {
            this.option = option;
        }

        public int getJmxPort() {
            return jmxPort;
        }

        public void setJmxPort(int jmxPort) {
            this.jmxPort = jmxPort;
        }

        public String path(){

            return ZKNode.NodesZNode.getPath(id);
        }
        public byte[] toJsonBytes(){

            return ZKNode.NodesZNode.encode(this);
        }


        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public Node build() {

            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "NodeInfo{" +
                    "id=" + id +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", jmxPort=" + jmxPort +
                    ", option=" + Joiner.on(",").withKeyValueSeparator("->").join(Optional.ofNullable(option).orElseGet(Collections::emptyMap)) +
                    ", version=" + version +
                    '}';
        }
    }

}
