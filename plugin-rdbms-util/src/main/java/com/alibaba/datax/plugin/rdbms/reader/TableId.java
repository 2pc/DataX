package com.alibaba.datax.plugin.rdbms.reader;

/**
 * Created by luping on 2017/11/1.
 */

public class TableId {


    private String catalogName;

    private String tableName;


    public TableId(String catalogName, String tableName) {
        super();
        this.catalogName = catalogName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
