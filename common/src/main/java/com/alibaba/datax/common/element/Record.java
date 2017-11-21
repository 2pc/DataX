package com.alibaba.datax.common.element;

import java.util.List;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {

	public List<Column> getColumns();

	public void setColumns(List<Column> columns) ;
	public void addColumn(Column column);

	public void setColumn(int i, final Column column);

	public Column getColumn(int i);

	public String toString();

	public int getColumnNumber();

	public int getByteSize();

	public int getMemorySize();
	
	public String getSchemaName();
	
	public void setSchemaName(String schemaName);
	
	public String getTableName();
	
	public void setTableName(String tableName);
	

}
