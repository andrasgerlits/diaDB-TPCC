package com.dianemodb.tpcc.schema;

import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;

public abstract class WarehouseBasedTable<R extends UserRecord> extends TpccBaseTable<R> {

	public WarehouseBasedTable(UserRecordTableId tableId, String name, Topology topology) {
		super(tableId, name, topology);
	}
	
	public WarehouseBasedTable(UserRecordTableId tableId, String name, Caching caching, Topology topology) {
		super(tableId, name, caching, topology);
	}

	public abstract RecordColumn<R, Short> getWarehouseIdColumn();
	

}
