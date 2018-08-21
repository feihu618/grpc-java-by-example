package com.nebutown.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ZKNode {

    static class ClusterZNode{
        private static final String BASE_PATH = "/clusters";

        static String getBasePath() {

            return BASE_PATH;
        }

        static String path() {
            return path(Config.getInstance().getClusterName());
        }

        static String path(String name) {

            return BASE_PATH +"/"+name;
        }

         static byte[] toJson() {
            try {
                return Json.encodeAsBytes(ImmutableMap.of("version","1"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("--- fatal error: Json.encodeAsBytes:");
            }
        }

         static Map<String, String> fromJson(byte[] bytes) {

             return Json.fromJson(new TypeReference<Map<String, String>>() {
             }, bytes);

        }
    }

    static class NodesZNode{

        static final String PATH = "/nodes";

        static final String NODES_SEQ_ID = PATH+"/seqid";

         static String getPath() {

            return ClusterZNode.path()+PATH;
        }

         static String getPath(int id) {

            return getPath()+"/"+id;
        }

        static String getSeqIdPath() {
             return ClusterZNode.path()+NODES_SEQ_ID;
        }

         static byte[] encode(Node.NodeInfo nodeInfo) {

             try {
                 return Json.encodeAsBytes(nodeInfo);
             } catch (JsonProcessingException e) {
                 e.printStackTrace();
                 throw new RuntimeException("--- encodeAsBytes:" + nodeInfo + " failed");
             }
         }

         static Node.NodeInfo decode(Integer nodeId, byte[] data) {

            return Json.fromJson(Node.NodeInfo.class, data);
        }
    }

    static class MasterZNode{

         static String path() {

             return ClusterZNode.path() + "/" + "master";
        }


        static String getPath(int id) {

            return path()+"/"+id;
        }

         static byte[] encode(Integer masterId, Long timestamp) {

             return Json.toJsonAsBytes(ImmutableMap.of("version", 1, "master_id", masterId, "timestamp", timestamp));
        }

         static Optional<Integer> decode(byte[] data) {
            return Optional.ofNullable(data)
                            .map(d -> Json.fromJson(new TypeReference<Map<String, Object>>() {}, d))
                            .map(map -> (Integer) map.get("master_id"));
        }
    }

    static class EpochZNode{

        static final String PATH = "/epoch";

         static String path() {

             return ClusterZNode.path()+PATH;
        }

         static byte[] encode(Integer epoch) {
            return epoch.toString().getBytes(Charsets.UTF_8);
        }

         static int decode(byte[] data) {
            return Integer.parseInt(new String(data, Charsets.UTF_8));
        }
    }

    static class BranchesZNode {

        static final String PATH = "/branches";

        static String path() {

            return ClusterZNode.path() + PATH;
        }

        static String getDataPath(Integer epoch) {

            return path() + "/" + epoch + "_data";
        }

        static String getBallotPath(Integer epoch) {

            return path() + "/" + epoch + "_ballot";
        }

        static String getBallotPath(Integer epoch, Integer nodeId) {

            return getBallotPath(epoch) + "/" + nodeId;
        }


        static String getCommitStatPath(Integer epoch) {

            return path() + "/" + epoch + "_commit_stat";
        }


        static byte[] encode(List<Integer> nodeIds) {

            return Json.toJsonAsBytes(nodeIds);
        }

        static ArrayList<Integer> decode(byte[] data) {

            return Json.fromJson(new TypeReference<ArrayList<Integer>>() {
            }, data);
        }

        static byte[] encode(boolean status) {

            return new byte[]{(byte) (status ? 1 : 0)};
        }

        static boolean decodeStatus(byte[] data) {

            return data[0] == 1;
        }
    }
}
