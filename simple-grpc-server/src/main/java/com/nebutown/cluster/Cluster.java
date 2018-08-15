package com.nebutown.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    public static class ClusterZNode {

        public static String path() {
            throw new UnsupportedOperationException();
        }
        public static byte[] toJson(String id) {
            try {
                return Json.encodeAsBytes(ImmutableMap.of("version","1", "id", id));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("--- fatal error: Json.encodeAsBytes:");
            }
        }

        public static Map<String, String> fromJson(byte[] bytes) {

            try {
                return Json.decodeObject(bytes, Map.class);
            } catch (IOException e) {
//                e.printStackTrace();
                LOG.error("--- fatal error: decodeObject:");
            }

            return null;
        }
    }

    public static class MasterZNode{


        public static String path() {
            throw new UnsupportedOperationException();
        }

        public static byte[] encode(Integer controllerId, Long timestamp) {
            throw new UnsupportedOperationException();
        }

        public static int decode(byte[] data) {
            throw new UnsupportedOperationException();
        }
    }

    public static class EpochZNode {
        public static String path() {
            throw new UnsupportedOperationException();
        }

        public static byte[] encode(Integer epoch) {
            throw new UnsupportedOperationException();
        }

        public static int decode(byte[] data) {
            throw new UnsupportedOperationException();
        }
    }

}
