package com.alibaba.datax.plugin.rdbms.commons;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.TableId;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;


public class DBZRecordMarker {

    private Configuration readerSliceConfig;

    private RecordSender recordSender;

    private TaskPluginCollector taskPluginCollector;

    private int fetchSize;

    protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

    public DBZRecordMarker(Configuration readerSliceConfig,
                           RecordSender recordSender,
                           TaskPluginCollector taskPluginCollector, int fetchSize) {

        this.readerSliceConfig = readerSliceConfig;
        this.recordSender =recordSender;
        this.taskPluginCollector = taskPluginCollector;
        this.fetchSize = fetchSize;
    }

    public void genrateRecord(ResultSet rs){


//        String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
//        String table = readerSliceConfig.getString(Key.TABLE);
//
//        PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);
//
//        LOG.info("Begin to read record by Sql: [{}\n] {}.",
//                querySql, basicMsg);
//        PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
//        queryPerfRecord.start();
//
//        Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
//                username, password);
//
//        // session config .etc related
//        DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
//                this.dataBaseType, basicMsg);
//
        int columnNumber = 0;
//        ResultSet rs = null;
        try {
//            rs = DBUtil.query(conn, querySql, fetchSize);
//            queryPerfRecord.end();

            ResultSetMetaData metaData = rs.getMetaData();
            columnNumber = metaData.getColumnCount();

//            //这个统计干净的result_Next时间
//            PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
//            allResultPerfRecord.start();

            long rsNextUsedTime = 0;
            long lastTime = System.nanoTime();
            TableId TableId =null;
            while (rs.next()) {

                rsNextUsedTime += (System.nanoTime() - lastTime);
                this.transportOneRecordWithSchema(recordSender, rs,
                        metaData, columnNumber, "utf-8", taskPluginCollector,TableId);
                lastTime = System.nanoTime();
            }

//            allResultPerfRecord.end(rsNextUsedTime);
//            //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
//            LOG.info("Finished read record by Sql: [{}\n] {}.",
//                    querySql, basicMsg);

        }catch (Exception e) {
 //           throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
        } finally {
 //           DBUtil.closeDBResources(null, conn);
        }
    }

    protected Record buildRecordWithSchema(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                           TaskPluginCollector taskPluginCollector, TableId tableId) {
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
//            if (IS_DEBUG) {
//                LOG.debug("read data " + record.toString()
//                        + " occur exception:", e);
//            }
            //TODO 这里识别为脏数据靠谱吗？
            taskPluginCollector.collectDirtyRecord(record, e);
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
        }
        return record;
    }

    protected Record transportOneRecordWithSchema(RecordSender recordSender, ResultSet rs,
                                                  ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                                  TaskPluginCollector taskPluginCollector,TableId tableId) {
        Record record = buildRecordWithSchema(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector,tableId);
        recordSender.sendToWriter(record);
        return record;
    }







}
