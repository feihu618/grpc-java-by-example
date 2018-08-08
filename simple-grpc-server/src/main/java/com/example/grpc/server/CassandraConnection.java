/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.grpc.server;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.util.Map;

/**
 * Set up a Casssandra connection
 **/

public class CassandraConnection {
    private final Cluster cluster;
    private final Session session;

    /**
     * <h1>CassandraConnection</h1>
     * <p>
     * Case class to hold a Cassandra cluster and session connection
     **/
    public CassandraConnection(Cluster cluster, Session session) {
        this.cluster = cluster;
        this.session = session;

    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    /*def apply(connectorConfig: AbstractConfig): CassandraConnection = {
    val cluster = getCluster(connectorConfig)
    val keySpace = connectorConfig.getString(CassandraConfigConstants.KEY_SPACE)
    val session = getSession(keySpace, cluster)
    new CassandraConnection(cluster = cluster, session = session)
  }*/

    public static Cluster getCluster(Map<String, Object> connectorConfig) {

        String contactPoints = (String) connectorConfig.get(CassandraConfigConstants.CONTACT_POINTS);
        Integer port = (Integer) connectorConfig.get(CassandraConfigConstants.PORT);
        Integer fetchSize = (Integer) connectorConfig.getOrDefault(CassandraConfigConstants.FETCH_SIZE, CassandraConfigConstants.FETCH_SIZE_DEFAULT);
        Cluster.Builder builder = Cluster
                .builder()
                .addContactPoints(contactPoints.split(","))
                .withPort(port)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .withQueryOptions(new QueryOptions().setFetchSize(fetchSize));

        //get authentication mode, only support NONE and USERNAME_PASSWORD for now
//    addAuthMode(connectorConfig, builder)

        //is ssl enable
//    addSSL(connectorConfig, builder)
        return builder.build();
    }

    public static void createKeyspace(
            Cluster cluster,
            String keyspaceName,
            String replicationStrategy,
            int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();

        Session session = cluster.newSession();
        try {
            session.execute(query);
        }finally {
            session.closeAsync();
        }

        return;

    }

    public static void createTable(Session session, String tableName) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append("(")
                .append("id text PRIMARY KEY, ")
                .append("object text,")
                .append("version text);");

        String query = sb.toString();
        session.execute(query);
    }

    /**
     * Get a Cassandra session
     *
     * @param keySpace A configuration to build the setting from
     * @param cluster  The cluster the get the session for
     **/
    public static Session getSession(String keySpace, Cluster cluster) {

        return cluster.connect(keySpace);
    }

    public static <T> Mapper<T> getMapper(Session session, Class<T> glass) {

        MappingManager manager = new MappingManager(session);
        return manager.mapper(glass);
    }


    /**
     * Add authentication to the connection builder
     *
     * @param connectorConfig The connector configuration to get the parameters from
     * @param builder         The builder to add the authentication to
     * @return The builder with authentication added.
     **/
/*
  private def addAuthMode(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val username = connectorConfig.getString(CassandraConfigConstants.USERNAME)
    val password = connectorConfig.getPassword(CassandraConfigConstants.PASSWD).value

    if (username.length > 0 && password.length > 0) {
      builder.withCredentials(username.trim, password.toString.trim)
    } else {
      logger.warn("Username and password not set.")
    }
    builder
  }
*/

    /**
     * Add SSL connection options to the connection builder
     *
     * @param connectorConfig The connector configuration to get the parameters from
     * @param builder         The builder to add the authentication to
     * @return The builder with SSL added.
     **/
/*  private def addSSL(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val ssl = connectorConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED).asInstanceOf[Boolean]
    if (ssl) {
      logger.info("Setting up SSL context.")
      val sslConfig = SSLConfig(
        trustStorePath = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_PATH),
        trustStorePass = connectorConfig.getPassword(CassandraConfigConstants.TRUST_STORE_PASSWD).value,
        keyStorePath = Some(connectorConfig.getString(CassandraConfigConstants.KEY_STORE_PATH)),
        keyStorePass = Some(connectorConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value),
        useClientCert = connectorConfig.getBoolean(CassandraConfigConstants.USE_CLIENT_AUTH),
        keyStoreType = connectorConfig.getString(CassandraConfigConstants.KEY_STORE_TYPE),
        trustStoreType = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_TYPE)
      )

      val context = SSLConfigContext(sslConfig)
      //val cipherSuites: Array[String] = Array("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
      val sSLOptions = new JdkSSLOptions.Builder
      //sSLOptions.withCipherSuites(cipherSuites)
      sSLOptions.withSSLContext(context)
      builder.withSSL(sSLOptions.build())
    } else {
      builder
    }
  }*/
}

