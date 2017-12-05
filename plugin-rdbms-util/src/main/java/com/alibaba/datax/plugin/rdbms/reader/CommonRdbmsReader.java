package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.commons.DBZRecordMarker;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.PreCheckTask;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.connector.mysql.SnapshotReader2;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CommonRdbmsReader {

    public static class Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        public Job(DataBaseType dataBaseType) {
            OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
            SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig);

            LOG.debug("After job init(), job config now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public void preCheck(Configuration originalConfig,DataBaseType dataBaseType) {
            /*检查每个表是否有读权限，以及querySql跟splik Key是否正确*/
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            List<Object> connList = queryConf.getList(Constant.CONN_MARK, Object.class);
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);
            ExecutorService exec;
            if (connList.size() < 10){
                exec = Executors.newFixedThreadPool(connList.size());
            }else{
                exec = Executors.newFixedThreadPool(10);
            }
            Collection<PreCheckTask> taskList = new ArrayList<PreCheckTask>();
            for (int i = 0, len = connList.size(); i < len; i++){
                Configuration connConf = Configuration.from(connList.get(i).toString());
                PreCheckTask t = new PreCheckTask(username,password,connConf,dataBaseType,splitPK);
                taskList.add(t);
            }
            List<Future<Boolean>> results = Lists.newArrayList();
            try {
                results = exec.invokeAll(taskList);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (Future<Boolean> result : results){
                try {
                    result.get();
                } catch (ExecutionException e) {
                    DataXException de = (DataXException) e.getCause();
                    throw de;
                }catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            exec.shutdownNow();
        }


        public List<Configuration> split(Configuration originalConfig,
                                         int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

    }

    public static class Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId=-1;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType) {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType,int taskGropuId, int taskId) {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }

        public void init(Configuration readerSliceConfig) {

			/* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            //ob10的处理
            if (this.jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING) && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() +":"+this.username;
                this.jdbcUrl = ss[2];
                LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

        }


        public  void startDBZRead(Configuration readerSliceConfig,
                                  RecordSender recordSender,
                                  TaskPluginCollector taskPluginCollector, int fetchSize){

            LOG.error("readerSliceConfig: "+readerSliceConfig);
            LOG.error("split_pk: "+readerSliceConfig.get(Key.SPLIT_PK));



            LOG.error("internalObj.getString(Key.KAFKA_TOPIC): "+readerSliceConfig.getString(Key.KAFKA_TOPIC));


            LOG.error("readerSliceConfig.getString(Key.KAFKA_TOPIC): "+readerSliceConfig.getString(Key.KAFKA_TOPIC));




            Map<String, String> props = new HashMap<String,String>();
            /*
            *     public final static String NAME = "name";
    public final static String CONNECTOR_CLASS = "connector.class";
    public final static String DATABASE_HOST = "database.hostname";
    public final static String DATABASE_PORT = "database.port";
    public final static String DATABASE_USER = "database.user";
    public final static String DATABASE_PASSWORD = "database.password";
    public final static String DATABASE_SERVER_ID = "database.server.id";
    public final static String DATABASE_SERVER_NAME = "database.server.name";
    public final static String DATABASE_WHITELIST = "database.whitelist";
    public final static String INCLUDE_SCHEMA_CHANGES = "include.schema.changes";
    public final static String KAFKA_BOOTSTRAP_SERVERS = "database.history.kafka.bootstrap.servers";
    public final static String KAFKA_TOPIC = "database.history.kafka.topic";*/
            props.put(Key.NAME,readerSliceConfig.getString(Key.NAME));
            props.put(Key.DBZ_CONNECTOR_CLASS,readerSliceConfig.getString(Key.CONNECTOR_CLASS));
            props.put(Key.DBZ_DATABASE_HOST,readerSliceConfig.getString(Key.DATABASE_HOST));
            props.put(Key.DBZ_DATABASE_PORT,readerSliceConfig.getString(Key.DATABASE_PORT));
            props.put(Key.DBZ_DATABASE_USER,readerSliceConfig.getString(Key.DATABASE_USER));
            props.put(Key.DBZ_DATABASE_PASSWORD,readerSliceConfig.getString(Key.DATABASE_PASSWORD));
            props.put(Key.DBZ_DATABASE_SERVER_ID,readerSliceConfig.getString(Key.DATABASE_SERVER_ID));
            props.put(Key.DBZ_DATABASE_SERVER_NAME,readerSliceConfig.getString(Key.DATABASE_SERVER_NAME));
            props.put(Key.DBZ_DATABASE_WHITELIST,readerSliceConfig.getString(Key.DATABASE_WHITELIST));
            props.put(Key.DBZ_INCLUDE_SCHEMA_CHANGES,readerSliceConfig.getString(Key.INCLUDE_SCHEMA_CHANGES));
            props.put(Key.DBZ_KAFKA_BOOTSTRAP_SERVERS,readerSliceConfig.getString(Key.KAFKA_BOOTSTRAP_SERVERS));
            props.put(Key.DBZ_KAFKA_TOPIC,readerSliceConfig.getString(Key.KAFKA_TOPIC));

            io.debezium.config.Configuration config = io.debezium.config.Configuration.from(props);


            LOG.error("config.getString(Key.KAFKA_TOPIC): "+config.getString(Key.KAFKA_TOPIC));
            System.out.print("config.getString(Key.KAFKA_TOPIC): "+config.getString(Key.KAFKA_TOPIC));


            System.out.print(JSON.toJSONString(readerSliceConfig));

            LOG.error("JSON.toJSONString(config): "+JSON.toJSONString(readerSliceConfig));

            MySqlTaskContext mySqlTaskContext = new MySqlTaskContext(config);

            SnapshotReader2 snapshotReader = new SnapshotReader2("full-import",mySqlTaskContext);


            DBZRecordMarker  dBZRecordMarker =  new DBZRecordMarker(readerSliceConfig,recordSender,taskPluginCollector,fetchSize);

            snapshotReader.execute(dBZRecordMarker);


        }

        // see
        public void startSnapShotRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String table = readerSliceConfig.getString(Key.TABLE);

            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql, basicMsg);

            LOG.info("end Begin to getConnection");
            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                    username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
                    this.dataBaseType, basicMsg);

            ResultSet rs = null;
            Statement stmt = null;
            boolean isLocked =false;
            boolean tableLocked =false;
            try {
                conn.setAutoCommit(false);
                stmt = conn.createStatement();
                //Step 0: disabling autocommit and enabling repeatable read transactions
                stmt.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

                try{
                    //Step 1: flush and obtain global read lock to prevent writes to database
                    stmt.execute("FLUSH TABLES WITH READ LOCK");
                    isLocked =true;

                }catch (SQLException sQLException1){

                }


                //Step 2: start transaction with consistent snapshot
                stmt.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");

                //Step 3 readBinlogPosition
                if(isLocked){
                    rs = stmt.executeQuery("SHOW MASTER STATUS");

                    while (rs.next()) {
                        String binlogFilename = rs.getString(1);
                        long binlogPosition = rs.getLong(2);
                        LOG.info("binlogFilename: "+binlogFilename+ " binlogPosition: "+binlogPosition);
                        if (rs.getMetaData().getColumnCount() > 4) {
                            String gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                            LOG.info("gtidSet: " +gtidSet);
                        }

                    }
                }
                // -------------------
                // Step 4 READ DATABASE NAMES
                // -------------------
                // Get the list of databases ...
                final List<String> databaseNames = new ArrayList<String>();
                rs = stmt.executeQuery("SHOW DATABASES");
                while(rs.next()){
                    databaseNames.add(rs.getString(1));
                }
                System.out.println("READ DATABASE NAMES 8");
                List<TableId> tableIds = new ArrayList<TableId>();
                final Map<String, List<TableId>> tableIdsByDbName = new HashMap<String, List<TableId>>();
                final Set<String> readableDatabaseNames = new HashSet<String>();


                // ----------------
                // READ TABLE NAMES
                // ----------------

                for (String dbName : databaseNames) {

                    System.out.println("READ DATABASE NAMES "+dbName);

                    String sql = "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'";
                    if(!dbName.equals("oml_backup")){
                        rs = stmt.executeQuery(sql);
                        System.out.println("READ DATABASE NAMES "+dbName+"   end");
                    }

                    while (rs.next()){
                        TableId tableId = new TableId(dbName, rs.getString(1));
                        tableIds.add(tableId);
                        List<TableId> l= new ArrayList<TableId>();
                        l.add(tableId);
                        tableIdsByDbName.putIfAbsent(dbName, l);

                    }

                    readableDatabaseNames.add(dbName);
                }



                boolean userHasPrivileges = false ;

                //LOCK TABLES and READ BINLOG POSITION
                if(!isLocked){
                    rs = stmt.executeQuery("SHOW GRANTS FOR CURRENT_USER");
                    while(rs.next()){
                        String grants = rs.getString(1);
                        LOG.info("grants###############" +grants);
                        System.out.println("grants###############" +grants);
                        if (grants.contains("ALL") || grants.contains("LOCK TABLES".toUpperCase())) {
                            System.out.println("GRANTS ok ");
                            userHasPrivileges=true;
                        }
                    }

                }

                if(!isLocked&&!userHasPrivileges){
              //      throw new IOException("user Has Privileges");
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR,"user not HasPrivileges");
                }

                // ------------------------------------
                // LOCK TABLES and READ BINLOG POSITION
                // ------------------------------------

                System.out.print("FLUSH TABLES WITH READ LOCK");
                StringBuilder allTables = new StringBuilder("FLUSH TABLES ");
                for (TableId tableId : tableIds) {
                    allTables.append(tableId.getCatalogName()).append(".").append(tableId.getTableName()).append(",");
                }

                allTables.subSequence(0, allTables.length()-2);

                allTables.append(" WITH READ LOCK");
                System.out.print("FLUSH TABLES WITH READ LOCK2: "+allTables);

                try{
                    stmt.execute(allTables.toString());
                }catch (Exception e){
                    System.out.println(e);
                }

                isLocked =true;
                tableLocked =true;
                System.out.print("before SHOW MASTER STATUS");

                rs = stmt.executeQuery("SHOW MASTER STATUS");
                System.out.print("SHOW MASTER STATUS");

                while (rs.next()) {
                    String binlogFilename = rs.getString(1);
                    long binlogPosition = rs.getLong(2);
                    System.out.println("binlogFilename: "+binlogFilename+ " binlogPosition: "+binlogPosition);
                    if (rs.getMetaData().getColumnCount() > 4) {
                        String gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                        System.out.println("gtidSet: " +gtidSet);
                    }

                }

                // ------
                // STEP 6  schema
                // ------


                // STEP 7
                if(isLocked){
                    if(tableLocked){

                    }else{
                        String  sql = "UNLOCK TABLES";
                        stmt.execute(sql);
                    }

                }

                // STEP 8
                System.out.print("STEP 8");

                Iterator<TableId> tableIdIter = tableIds.iterator();

                stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(Integer.MIN_VALUE);

                PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
                queryPerfRecord.start();
                while (tableIdIter.hasNext()) {
                    TableId  tableId = tableIdIter.next();
                    if("test".equals(tableId.getCatalogName())){
	                    String sql = "USE " + quote(tableId.getCatalogName()) + ";";
	                    //stmt.execute(sql);
	                    String  sqlTmp = "SELECT * FROM " + quote(tableId);
	                    rs = stmt.executeQuery(sqlTmp);
	
	                    System.out.println(sqlTmp);
	                    //rs = stmt.executeQuery(querySql);
	                    ResultSetMetaData metaData = rs.getMetaData();
	                    int columnNumber = metaData.getColumnCount();
	                    while(rs.next()){
	
	                        this.transportOneRecordWithSchema(recordSender,rs,rs.getMetaData(),columnNumber,mandatoryEncoding,taskPluginCollector,tableId);
	                        //System.out.println(transportOneRecord(rs, rs.getMetaData(), columnNumber, "utf-8"));
	                    }
                    }
                }

                System.out.println("end ##################");


            }catch (SQLException sQLException){

            }


        }

        private  String quote(String dbName) {
            return "`" + dbName + "`";
        }

        protected  String quote(TableId id) {
            return quote(id.getCatalogName()) + "." + quote(id.getTableName());
        }

        public void startRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {

            String queryType = readerSliceConfig.getString(Key.QUERY_TYPE);

           // if(Key.QUERY_SNAPSHOT_TYPE.equals(queryType)){
            startDBZRead(readerSliceConfig,recordSender,taskPluginCollector,fetchSize);
//                startSnapShotRead(readerSliceConfig,recordSender,taskPluginCollector,fetchSize);
//            }else{
//                startJdbcSqlRead(readerSliceConfig,recordSender,taskPluginCollector,fetchSize);
//            }
        }


        public void startJdbcSqlRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String table = readerSliceConfig.getString(Key.TABLE);

            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql, basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                    username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
                    this.dataBaseType, basicMsg);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.",
                        querySql, basicMsg);

            }catch (Exception e) {
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }
        
        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, 
                ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, 
                TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector); 
            recordSender.sendToWriter(record);
            return record;
        }
        protected Record transportOneRecordWithSchema(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                            TaskPluginCollector taskPluginCollector,TableId tableId) {
            Record record = buildRecordWithSchema(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector,tableId);
            recordSender.sendToWriter(record);
            return record;
        }
        protected Record buildRecord(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
        		TaskPluginCollector taskPluginCollector) {
        	Record record = recordSender.createRecord();


            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        String rawData;
                        if(StringUtils.isBlank(mandatoryEncoding)){
                            rawData = rs.getString(i);
                        }else{
                            rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : 
                                rs.getBytes(i)), mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if(rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;

                    default:
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.UNSUPPORTED_TYPE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                metaData.getColumnName(i),
                                                metaData.getColumnType(i),
                                                metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

        protected Record buildRecordWithSchema(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector,TableId tableId) {
            Record record = recordSender.createRecord();
            record.setSchemaName(tableId.getCatalogName());
            record.setTableName(tableId.getTableName());

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if(StringUtils.isBlank(mandatoryEncoding)){
                                rawData = rs.getString(i);
                            }else{
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                        rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if(rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            DBUtilErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }
    }

}
