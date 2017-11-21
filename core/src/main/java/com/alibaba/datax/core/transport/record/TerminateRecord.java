package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;

import java.util.List;

/**
 * 作为标示 生产者已经完成生产的标志
 * 
 */
public class TerminateRecord implements Record {
	private final static TerminateRecord SINGLE = new TerminateRecord();

	public List<Column> getColumns(){
		return null;
	};
	public void setColumns(List<Column> columns){} ;
	private TerminateRecord() {
	}

	public static TerminateRecord get() {
		return SINGLE;
	}

	@Override
	public void addColumn(Column column) {
	}

	@Override
	public Column getColumn(int i) {
		return null;
	}

	@Override
	public int getColumnNumber() {
		return 0;
	}

	@Override
	public int getByteSize() {
		return 0;
	}

	@Override
	public int getMemorySize() {
		return 0;
	}

	@Override
	public void setColumn(int i, Column column) {
		return;
	}

	@Override
	public String getSchemaName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchemaName(String schemaName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTableName(String tableName) {
		// TODO Auto-generated method stub
		
	}
}
