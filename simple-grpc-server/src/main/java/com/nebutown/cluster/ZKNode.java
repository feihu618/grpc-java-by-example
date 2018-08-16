package com.nebutown.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ZKNode {
    private static final Logger LOG = LoggerFactory.getLogger(ZKNode.class);

    static class ClusterZNode{
        private static final String CLUSTER_ID = "/cluster/id/"+Config.getProperty("cluster_id");
        static String path() {
            return CLUSTER_ID;
        }

         static byte[] toJson(String id) {
            try {
                return Json.encodeAsBytes(ImmutableMap.of("version","1", "id", id));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("--- fatal error: Json.encodeAsBytes:");
            }
        }

         static Map<String, String> fromJson(byte[] bytes) {

            try {
                return Json.decodeObject(bytes, Map.class);
            } catch (IOException e) {
//                e.printStackTrace();
                LOG.error("--- fatal error: decodeObject:");
            }

            return null;
        }
    }

    static class NodesZNode{

        static final String PATH = "/nodes";

         static String getPath() {

            return ClusterZNode.path()+PATH;
        }

         static String getPath(int id) {

            return getPath()+"/"+id;
        }

         static byte[] encode(Node.NodeInfo nodeInfo) {

            throw new UnsupportedOperationException();
        }

         static Node.NodeInfo decode(Integer nodeId, byte[] data) {

            throw new UnsupportedOperationException();
        }
    }

    static class MasterZNode{

         static String path() {

             return ClusterZNode.path() + "/" + "master";
        }

         static byte[] encode(Integer masterId, Long timestamp) {
            throw new UnsupportedOperationException();
        }

         static int decode(byte[] data) {
            throw new UnsupportedOperationException();
        }
    }

    static class EpochZNode{


         static String path() {
            throw new UnsupportedOperationException();
        }

         static byte[] encode(Integer epoch) {
            throw new UnsupportedOperationException();
        }

         static int decode(byte[] data) {
            throw new UnsupportedOperationException();
        }
    }
}
