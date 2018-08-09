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

package com.example.grpc.client;

import com.example.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Random;
import java.util.UUID;

/**
 * Created by rayt on 5/16/16.
 */
public class MyGrpcClient {
  public static void main(String[] args) throws InterruptedException {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
        .usePlaintext(true)
        .build();

    Random rand = new Random();

    CassandraRestfulServiceGrpc.CassandraRestfulServiceBlockingStub stub =
            CassandraRestfulServiceGrpc.newBlockingStub(channel);

    String key = UUID.randomUUID().toString();

    TResponse helloResponse = stub.exec(
        TRequest.newBuilder()
            .setType(RequestType.CREATE)
                .setRecord(TRecord.newBuilder().setKey(key).setValue("hello world"+rand.nextInt()).setVersion(1l).build())
            .build());

    System.out.println(helloResponse);


    helloResponse = stub.exec(
            TRequest.newBuilder()
                    .setType(RequestType.GET)
                    .setRecord(TRecord.newBuilder().setKey(key).build())
                    .build());

    System.out.println(helloResponse);

    channel.shutdown();
  }
}
