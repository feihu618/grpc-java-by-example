package com.nebutown.persistency;

import com.example.grpc.server.RID;
import com.google.common.collect.Lists;
import com.nebutown.cluster.Cluster;
import com.nebutown.cluster.Json;
import com.nebutown.cluster.Node;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class GTxDataBox  {
    private Cluster cluster;

    private ConcurrentHashMap<RID, List<Node>> se = new ConcurrentHashMap<>();


    public void commit(RID request) {

        List<Node> nodes = se.remove(request);
        if (nodes.parallelStream()
                .map(node -> node.tryCommit(request))
                .allMatch(p -> p == Boolean.TRUE))
            nodes.parallelStream().forEach(node -> node.commit(request));
    }


    public <T> T getData(RID request, Class<T> clazz, UUID key) {

        Node node = cluster.getNode(key.toString());

        String value = node.getRecord(request, key);

        return Json.fromJson(clazz, value);
    }



    public <T> void putData(RID request, UUID key, T data) {

        Node node = cluster.getNode(key.toString());

        node.putRecord(request, key, data);

        se.compute(request, (rid, list)->{

            return
                Optional.ofNullable(list)
                        .map(ls -> {ls.add(node);return ls;})
                        .orElseGet(()-> Lists.newArrayList(node));
        });
    }


}
