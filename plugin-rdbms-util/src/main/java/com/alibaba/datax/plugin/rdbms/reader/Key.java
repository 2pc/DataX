package com.alibaba.datax.plugin.rdbms.reader;

/**
 * 编码，时区等配置，暂未定.
 */
public final class Key {
    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";
    
    public final static String MANDATORY_ENCODING = "mandatoryEncoding";

    // 是数组配置
    public final static String COLUMN = "column";
    
    public final static String COLUMN_LIST = "columnList";

    public final static String WHERE = "where";

    public final static String HINT = "hint";

    public final static String SPLIT_PK = "splitPk";
    
    public final static String SPLIT_MODE = "splitMode";
    
    public final static String SAMPLE_PERCENTAGE = "samplePercentage";

    public final static String QUERY_SQL = "querySql";

    public final static String SPLIT_PK_SQL = "splitPkSql";


    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";

    public final static String CHECK_SLAVE = "checkSlave";

	public final static String SESSION = "session";

	public final static String DBNAME = "dbName";

    public final static String DRYRUN = "dryRun";

    public final static String QUERY_TYPE = "queryType";

    public final static String QUERY_SNAPSHOT_TYPE = "snapshot";

    /**
     *
     *
     "name": "inventory-connector2",
     "connector.class": "io.debezium.connector.mysql.MySqlConnector",
     "database.hostname": "172.28.3.159",
     "database.port": "3306",
     "database.user": "canal",
     "database.password": "canal",
     "database.server.id": "23333",
     "database.server.name": "23333",
     "database.whitelist": "test",
     "include.schema.changes": "false",
     "database.history.kafka.bootstrap.servers": "172.28.3.158:9092",
     "database.history.kafka.topic":"dbhistory.fullfillment",
     */
    /* datax configuration 层级关系不能用 "."*/
    public final static String NAME = "name";
    public final static String CONNECTOR_CLASS = "connector_class";
    public final static String DATABASE_HOST = "database_hostname";
    public final static String DATABASE_PORT = "database_port";
    public final static String DATABASE_USER = "database_user";
    public final static String DATABASE_PASSWORD = "database_password";
    public final static String DATABASE_SERVER_ID = "database_server_id";
    public final static String DATABASE_SERVER_NAME = "database_server_name";
    public final static String DATABASE_WHITELIST = "database_whitelist";
    public final static String INCLUDE_SCHEMA_CHANGES = "include_schema_changes";
    public final static String KAFKA_BOOTSTRAP_SERVERS = "database_history_kafka_bootstrap_servers";
    public final static String KAFKA_TOPIC = "database_history_kafka_topic";


    public final static String DBZ_NAME = "name";
    public final static String DBZ_CONNECTOR_CLASS = "connector.class";
    public final static String DBZ_DATABASE_HOST = "database.hostname";
    public final static String DBZ_DATABASE_PORT = "database.port";
    public final static String DBZ_DATABASE_USER = "database.user";
    public final static String DBZ_DATABASE_PASSWORD = "database.password";
    public final static String DBZ_DATABASE_SERVER_ID = "database.server.id";
    public final static String DBZ_DATABASE_SERVER_NAME = "database.server.name";
    public final static String DBZ_DATABASE_WHITELIST = "database.whitelist";
    public final static String DBZ_INCLUDE_SCHEMA_CHANGES = "include.schema.changes";
    public final static String DBZ_KAFKA_BOOTSTRAP_SERVERS = "database.history.kafka.bootstrap.servers";
    public final static String DBZ_KAFKA_TOPIC = "database.history.kafka.topic";




}