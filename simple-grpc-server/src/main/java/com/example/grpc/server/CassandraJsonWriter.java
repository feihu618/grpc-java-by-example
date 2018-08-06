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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <h1>CassandraJsonWriter</h1>
 * Cassandra Json writer for Kafka connect
 * Writes a list of Kafka connect sink records to Cassandra using the JSON support.
 */
public class CassandraJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(CassandraJsonWriter.class);
    private Session session;

    public CassandraJsonWriter(CassandraConnection connection, Map<String, Object> settings) {

//      initialize(settings.taskRetries, settings.errorPolicy)


//      CassandraUtils.checkCassandraTables(session.getCluster, settings.kcqls, session.getLoggedKeyspace)
    }


    /**
     * Get a connection to cassandra based on the config
     **/
    private Optional<Session> getSession() {
    /*val t = Try(connection.cluster.connect(settings.keySpace))
    handleTry[Session](t)*/

        throw new UnsupportedOperationException("no support now");
    }

    /**
     * Cache the preparedStatements per topic rather than create them every time
     * Each one is an insert statement aligned to topics.
     *
     * @return A Map of topic->(target -> preparedStatements).
     **/
/*  private def cachePreparedStatements = {
    settings.kcqls
      .groupBy(_.getSource)
      .map { case (topic, kcqls) =>
        val innerMap = kcqls.foldLeft(Map.empty[String, (PreparedStatement, Kcql)]) { case (map, k) =>
          val table = k.getTarget
          val ttl = k.getTTL
          logger.info(s"Preparing statements for $topic->$table")
          map + (table -> (getPreparedStatement(table, ttl).get, k))
        }

        topic -> innerMap
      }
  }*/

/*  private def cacheDeleteStatement: Option[PreparedStatement] = {
    if (settings.deleteEnabled)
        Some(session.prepare(settings.deleteStatement))
    else None
  }*/

    /**
     * Build a preparedStatement for the given topic.
     *
     * @param table The table name to prepare the statement for.
     * @return A prepared statement for the given topic.
     **/
    private PreparedStatement getPreparedStatement(String table, Long ttl) {

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ${session.getLoggedKeyspace}.$table JSON ?");

     /* if (settings.defaultValueStrategy.getOrElse(DefaultValueServeStrategy.NULL) == DefaultValueServeStrategy.UNSET)
        sb.append(" DEFAULT UNSET")*/
        if (ttl > 0L)
            sb.append(" USING TTL $ttl");

        PreparedStatement statement = session.prepare(sb.toString());
//      statement.setConsistencyLevel(settings)
        return statement;
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
            if (session.isClosed()) {
                logger.error("Session is closed attempting to reconnect to keySpace ${settings.keySpace}");
            }


            records.forEach(record -> {

                if (record.getObject() != null)
                    insert(record);
                else
                    delete(record);
            });
        }
    }


    private void insert(Record record) {
        throw new UnsupportedOperationException();
    }

    private void delete(Record record) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closed down the driver session and cluster.
     **/
    public void close() {
        logger.info("Shutting down Cassandra driver session and cluster.");
        session.close();
        session.getCluster().close();
    }
}