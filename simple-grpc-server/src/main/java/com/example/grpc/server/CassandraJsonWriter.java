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

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;


/**
 * <h1>CassandraJsonWriter</h1>
 * Cassandra Json writer for Kafka connect
 * Writes a list of Kafka connect sink records to Cassandra using the JSON support.
 */
public class CassandraJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(CassandraJsonWriter.class);
    private Supplier<Cluster>  supplier;

    public CassandraJsonWriter(CassandraConnection connection, Map<String, Object> settings) {

//      initialize(settings.taskRetries, settings.errorPolicy)

//      CassandraUtils.checkCassandraTables(cluster.getCluster, settings.kcqls, cluster.getLoggedKeyspace)

    }

    public CassandraJsonWriter(Supplier<Cluster> supplier) {

//      initialize(settings.taskRetries, settings.errorPolicy)

//      CassandraUtils.checkCassandraTables(cluster.getCluster, settings.kcqls, cluster.getLoggedKeyspace)
        this.supplier = supplier;
    }


    /**
     * Get a connection to cassandra based on the config
     **/
    private Optional<Session> getCluster() {
    /*val t = Try(connection.cluster.connect(settings.keySpace))
    handleTry[Session](t)*/

        throw new UnsupportedOperationException("no support now");
    }


    /**
     * Build a preparedStatement for the given topic.
     *
     * @param table The table name to prepare the statement for.
     * @return A prepared statement for the given topic.
     **/
    private PreparedStatement getPreparedStatement(String table, Long ttl) {

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ${cluster.getLoggedKeyspace}.$table JSON ?");

     /* if (settings.defaultValueStrategy.getOrElse(DefaultValueServeStrategy.NULL) == DefaultValueServeStrategy.UNSET)
        sb.append(" DEFAULT UNSET")*/
        if (ttl > 0L)
            sb.append(" USING TTL $ttl");

        PreparedStatement statement = null/*cluster.prepare(sb.toString())*/;
//      statement.setConsistencyLevel(settings)
        return statement;
    }

    public Record get(String id) {

        final Optional<Record> first = IntStream.range(0, 3)
                .mapToObj(i -> {

                    Session session = getSession0(i);
                    System.out.println("---next i>>" + i);
                    return get0(session, id);
                })
                .filter(Objects::nonNull)
                .findFirst();

        return first.orElseThrow(() -> new IllegalStateException("--- not found"));
    }

    private Record get0(Session session, String id) {


        try{

            Statement stmt = select().json().from("duker", "record").where(eq("id", id));
            Row row = session.execute(stmt).one();
            System.out.println("***************************************");
            System.out.println("---row>>>>>"+row);
            System.out.println("***************************************");

            final Record record = row.get("[json]", Record.class);

            return record;
        }catch (Exception e){

            e.printStackTrace();
        }finally {
            if (session != null)
                session.closeAsync();
        }

        return null;
    }


    /**
     * Write SinkRecords to Cassandra (aSync per topic partition) in Json.
     *
     * @param records A list of SinkRecords from Kafka Connect to write.
     **/
    public void write(List<Record> records) {
        if (records.isEmpty()) {
            logger.debug("No records received.");
        } else {

            logger.debug("Received ${records.size} records.");

            //is the connection still alive
     /*       if (cluster.isClosed()) {
                logger.error("Session is closed attempting to reconnect to keySpace ${settings.keySpace}");
            }*/


            records.forEach(record -> {

                if (record.getObject() != null)
                    insert(record);
                else
                    delete(record);
            });
        }
    }


    private void insert(Record record) {
                // Build and execute a simple statement
        try{

            IntStream.range(0, 3)
                    .filter(i -> {

                        Session session  =  getSession0(i);
                        System.out.println("---next i>>"+i);
                        return insert0(session, record);
                    })
                    .findFirst();



        }catch (Exception e){

            e.printStackTrace();
        }finally {

        }

    }

    private Session getSession0(int count) {

        try{
            if (count > 3)
                throw new IllegalStateException("--- illegal state");

            if (count == 0) {

                return supplier.get().connect();
            }else if (count > 1){


                return supplier.get().connect();
            }

        }catch (Exception e){
            //ignore
        }

        return getSession0(count + 1);
    }

    private boolean insert0(Session session, Record record) {
        // Build and execute a simple statement
        Statement stmt =
                insertInto("duker", "record")
                        .value("id", record.getId())
                        .value("object", record.getObject())
                        .value("version", record.getVersion());

        try{
            session.execute(stmt);
            return true;
        }catch (Exception e){

            e.printStackTrace();
        }finally {
            if (session != null)
                session.closeAsync();
        }

        return false;
    }

    private void delete(Record record) {


    }

    /**
     * Closed down the driver cluster and cluster.
     **/
/*    public void close() {
        logger.info("Shutting down Cassandra driver cluster and cluster.");
        cluster.close();
        cluster.getCluster().close();
    }*/
}
