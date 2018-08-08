/*
 * Copyright 2016 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.grpc.server;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.example.grpc.CassandraRestfulServiceGrpc;
import com.example.grpc.TRecord;
import com.example.grpc.TRequest;
import com.example.grpc.TResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

/**
 * Created by rayt on 5/16/16.
 */
public class MyGrpcServer {
    static public void main(String[] args) throws IOException, InterruptedException {

        final Mapper<Record> mapper = initCassandra(ImmutableMap.of("keyspace", "duker", "table", "record1"));

        Server server = ServerBuilder.forPort(8080)
                .addService(new GreetingServiceImpl(mapper)).build();


        System.out.println("Starting server...");
        server.start();
        System.out.println("Server started!");
        server.awaitTermination();
    }

    private static Mapper<Record> initCassandra(Map<String, Object> properties) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(new InetSocketAddress("127.0.0.1", 9042))
                    .build();

            String keyspace = (String) properties.get("keyspace");
            String table = (String) properties.get("table");
            Session session = null;

            if ((Boolean)properties.getOrDefault("auto", Boolean.TRUE)) {

                CassandraConnection.createKeyspace(cluster, keyspace, "SimpleStrategy", 1);

                session = CassandraConnection.getSession(keyspace, cluster);
                CassandraConnection.createTable(session, table);

            }


            if (session == null)
                session = CassandraConnection.getSession(keyspace, cluster);

            return CassandraConnection.getMapper(session, Record.class);



        } catch (Throwable e) {

            e.printStackTrace();
        } finally {
            if (cluster != null) cluster.close();
        }

        return null;
    }

    public static class GreetingServiceImpl extends CassandraRestfulServiceGrpc.CassandraRestfulServiceImplBase {
        private final Mapper<Record> mapper;;

        public GreetingServiceImpl(Mapper<Record> mapper) {
            this.mapper = mapper;
        }

        @Override
         public void exec(TRequest request, StreamObserver<TResponse> responseObserver) {
           System.out.println("-----"+request);

            TResponse tResponse = null;
            switch (request.getType()) {

                case DEL:

                    mapper.delete(Record.of(request.getRecord()));
                    tResponse = TResponse.newBuilder().setStatus("ok").build();
                    break;
                case GET:

                    final Record record = mapper.get(Record.of(request.getRecord()));
                    tResponse = TResponse.newBuilder().setRecord(to0(record)).setStatus("ok").build();
                    break;

                case UPDATE:
                case CREATE:

                    mapper.save(Record.of(request.getRecord()));
                    tResponse = TResponse.newBuilder().setStatus("ok").build();
                    break;

                case UNRECOGNIZED:
                    throw new IllegalArgumentException("--- UNRECOGNIZED");
            }

            responseObserver.onNext(tResponse);
            responseObserver.onCompleted();

         }

        private TRecord to0(Record record) {

            return TRecord.newBuilder().setKey(record.getId()).setValue(record.getObject()).setVersion(record.getVersion()).build();
        }

    }
}
