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

/**
 * Created by andrew@datamountaineer.com on 25/04/16.
 * stream-reactor
 */

/**
 * Holds the constants used in the config.
 */

interface CassandraConfigConstants {
    String CONNECTOR_PREFIX = "connect.cassandra";

    String USERNAME_PASSWORD = "username_password";
    String NONE = "none";
    String PAYLOAD = "payload";
    Integer DEFAULT_POLL_INTERVAL = 1000;

    String POLL_INTERVAL = "$CONNECTOR_PREFIX.import.poll.interval";
    String POLL_INTERVAL_DOC = "The polling interval between queries against tables for bulk mode.";

    String KEY_SPACE = "$CONNECTOR_PREFIX.key.space";
    String KEY_SPACE_DOC = "Keyspace to write to.";

    String CONTACT_POINTS = "$CONNECTOR_PREFIX.contact.points";
    String CONTACT_POINT_DOC = "Initial contact point host for Cassandra including port.";
    String CONTACT_POINT_DEFAULT = "localhost";

    String PORT = "$CONNECTOR_PREFIX.port";
    String PORT_DEFAULT = "9042";
    String PORT_DOC = "Cassandra native port.";

    String INSERT_JSON_PREFIX = "INSERT INTO ";
    String INSERT_JSON_POSTFIX = " JSON '?';";

    String USERNAME = "$CONNECTOR_PREFIX.$USERNAME_SUFFIX";
    String USERNAME_DOC = "Username to connect to Cassandra with.";
    String USERNAME_DEFAULT = "cassandra.cassandra";

    String PASSWD = "$CONNECTOR_PREFIX.$PASSWORD_SUFFIX";
    String PASSWD_DOC = "Password for the username to connect to Cassandra with.";
    String PASSWD_DEFAULT = "cassandra";

    String SSL_ENABLED = "$CONNECTOR_PREFIX.ssl.enabled";
    String SSL_ENABLED_DOC = "Secure Cassandra driver connection via SSL.";
    String SSL_ENABLED_DEFAULT = "false";

    String TRUST_STORE_PATH = "$CONNECTOR_PREFIX.trust.store.path";
    String TRUST_STORE_PATH_DOC = "Path to the client Trust Store.";
    String TRUST_STORE_PASSWD = "$CONNECTOR_PREFIX.trust.store.password";
    String TRUST_STORE_PASSWD_DOC = "Password for the client Trust Store.";
    String TRUST_STORE_TYPE = "$CONNECTOR_PREFIX.trust.store.type";
    String TRUST_STORE_TYPE_DOC = "Type of the Trust Store, defaults to JKS";
    String TRUST_STORE_TYPE_DEFAULT = "JKS";

    String USE_CLIENT_AUTH = "$CONNECTOR_PREFIX.ssl.client.cert.auth";
    String USE_CLIENT_AUTH_DEFAULT = "false";
    String USE_CLIENT_AUTH_DOC = "Enable client certification authentication by Cassandra. Requires KeyStore options to be set.";

    String KEY_STORE_PATH = "$CONNECTOR_PREFIX.key.store.path";
    String KEY_STORE_PATH_DOC = "Path to the client Key Store.";
    String KEY_STORE_PASSWD = "$CONNECTOR_PREFIX.key.store.password";
    String KEY_STORE_PASSWD_DOC = "Password for the client Key Store";
    String KEY_STORE_TYPE = "$CONNECTOR_PREFIX.key.store.type";
    String KEY_STORE_TYPE_DOC = "Type of the Key Store, defauts to JKS";
    String KEY_STORE_TYPE_DEFAULT = "JKS";

    //source

    String BATCH_SIZE = "$CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX";
    String BATCH_SIZE_DOC = "The number of records the source task should drain from the reader queue.";
    Integer BATCH_SIZE_DEFAULT = 100;

    String FETCH_SIZE = "$CONNECTOR_PREFIX.fetch.size";
    String FETCH_SIZE_DOC = "The number of records the Cassandra driver will return at once.";
    Integer FETCH_SIZE_DEFAULT = 5000;

    String READER_BUFFER_SIZE = "$CONNECTOR_PREFIX.task.buffer.size";
    String READER_BUFFER_SIZE_DOC = "The size of the queue as read writes to.";
    Integer READER_BUFFER_SIZE_DEFAULT = 10000;

    String TIME_SLICE_MILLIS = "$CONNECTOR_PREFIX.time.slice.ms";
    String TIME_SLICE_MILLIS_DOC = "The range of time in milliseconds the source task the timestamp/timeuuid will use for query";
    Integer TIME_SLICE_MILLIS_DEFAULT = 10000;

    String ALLOW_FILTERING = "$CONNECTOR_PREFIX.import.allow.filtering";
    String ALLOW_FILTERING_DOC = "Enable ALLOW FILTERING in incremental selects.";
    boolean ALLOW_FILTERING_DEFAULT = true;

    //for the source task, the connector will set this for the each source task
    String ASSIGNED_TABLES = "$CONNECTOR_PREFIX.assigned.tables";
    String ASSIGNED_TABLES_DOC = "The tables a task has been assigned.";

    String MISSING_KEY_SPACE_MESSAGE = "$KEY_SPACE must be provided.";
    String SELECT_OFFSET_COLUMN = "___kafka_connect_offset_col";

    String ERROR_POLICY = "$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX";
    String ERROR_POLICY_DOC = "";
    String ERROR_POLICY_DEFAULT = "THROW";

    String ERROR_RETRY_INTERVAL = "$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX";
    String ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries.";
    String ERROR_RETRY_INTERVAL_DEFAULT = "60000";

    String NBR_OF_RETRIES = "$CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX";
    String NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again.";
    Integer NBR_OF_RETIRES_DEFAULT = 20;

    String KCQL = "$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX";
    String KCQL_DOC = "KCQL expression describing field selection and routes.";

    String THREAD_POOL_CONFIG = "$CONNECTOR_PREFIX.$THREAD_POLL_PROP_SUFFIX";
    String THREAD_POOL_DOC = "";
    String THREAD_POOL_DISPLAY = "Thread pool size";
    Integer THREAD_POOL_DEFAULT = 0;

    String CONSISTENCY_LEVEL_CONFIG = "$CONNECTOR_PREFIX.$CONSISTENCY_LEVEL_PROP_SUFFIX";
    String CONSISTENCY_LEVEL_DOC = "";
    String CONSISTENCY_LEVEL_DISPLAY = "Consistency Level";
    String CONSISTENCY_LEVEL_DEFAULT = "";

    String PROGRESS_COUNTER_ENABLED = "";
    String PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed";
    boolean PROGRESS_COUNTER_ENABLED_DEFAULT = false;
    String PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter";

    String DELETE_ROW_PREFIX = "delete";

    String DELETE_ROW_ENABLED = "$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.enabled";
    String DELETE_ROW_ENABLED_DOC = "Enables row deletion from cassandra";
    boolean DELETE_ROW_ENABLED_DEFAULT = false;
    String DELETE_ROW_ENABLED_DISPLAY = "Enable delete row";

    String DELETE_ROW_STATEMENT = "$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.statement";
    String DELETE_ROW_STATEMENT_DOC = "Delete statement for cassandra";
    String DELETE_ROW_STATEMENT_DEFAULT = "";
    String DELETE_ROW_STATEMENT_DISPLAY = "Enable delete row";
    String DELETE_ROW_STATEMENT_MISSING = "If $DELETE_ROW_ENABLED is true, $DELETE_ROW_STATEMENT is required.";

    String DELETE_ROW_STRUCT_FLDS = "$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.struct_flds";
    String DELETE_ROW_STRUCT_FLDS_DOC = "Fields in the key struct data type used in there delete statement. Comma-separated in the order they are found in $DELETE_ROW_STATEMENT. Keep default value to use the record key as a primitive type.";
    String DELETE_ROW_STRUCT_FLDS_DEFAULT = "";
    String DELETE_ROW_STRUCT_FLDS_DISPLAY = "Field names in Key Struct";

    String TIMESLICE_DURATION = "$CONNECTOR_PREFIX.slice.duration";
    Integer TIMESLICE_DURATION_DEFAULT = 10000;
    String TIMESLICE_DURATION_DOC = "Duration to query for in target Cassandra table. Used to restrict query timestamp span";

    String TIMESLICE_DELAY = "$CONNECTOR_PREFIX.slice.delay.ms";
    Integer TIMESLICE_DELAY_DEFAULT = 30000;
    String TIMESLICE_DELAY_DOC = "The delay between the current time and the time range of the query. Used to insure all of the data in the time slice is available";

    String INITIAL_OFFSET = "$CONNECTOR_PREFIX.initial.offset";
    String INITIAL_OFFSET_DEFAULT = "1900-01-01 00:00:00.0000000Z";
    String INITIAL_OFFSET_DOC = "The initial timestamp to start querying in Cassandra from (yyyy-MM-dd HH:mm:ss.SSS'Z'). Default 1900-01-01 00:00:00.0000000Z";

    String MAPPING_COLLECTION_TO_JSON = "$CONNECTOR_PREFIX.mapping.collection.to.json";
    String MAPPING_COLLECTION_TO_JSON_DOC = "Mapping columns with type Map, List and Set like json";
    boolean MAPPING_COLLECTION_TO_JSON_DEFAULT = true;

    String DEFAULT_VALUE_SERVE_STRATEGY_PROPERTY = "$CONNECTOR_PREFIX.default.value";
    String DEFAULT_VALUE_SERVE_STRATEGY_DOC = "";

    String DEFAULT_VALUE_SERVE_STRATEGY_DEFAULT = "";
    String DEFAULT_VALUE_SERVE_STRATEGY_DISPLAY = "Default value serve strategy";
}
