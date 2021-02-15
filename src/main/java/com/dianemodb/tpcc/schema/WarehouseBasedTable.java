package com.dianemodb.tpcc.schema;

import java.util.List;

import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.h2impl.H2RangeBasedDistributedIndex;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.query.IndexColumnDefinition;
import com.dianemodb.query.RangeBasedDistributedIndex;

public abstract class WarehouseBasedTable<R extends UserRecord> extends TpccBaseTable<R> {

	private final RangeBasedDistributedIndex<R> distributionIndex;
	protected final IndexColumnDefinition<R, Short> warehouseIndexColumnDefinition;
	
	public WarehouseBasedTable(UserRecordTableId tableId, String name, Topology topology) {
		// default caching is CACHED
		this(tableId, name, Caching.CACHED, topology);
	}
	
	public WarehouseBasedTable(UserRecordTableId tableId, String name, Caching caching, Topology topology) {
		super(tableId, name, caching, topology);
		
		warehouseIndexColumnDefinition = 
				new IndexColumnDefinition<>(
						getWarehouseIdColumn(), 
						WarehouseTable.getWarehouseDistributionRule()
				);
		
		this.distributionIndex = 
				new H2RangeBasedDistributedIndex<>(
						topology, 
						this, 
						List.of(warehouseIndexColumnDefinition)
				);
	}

	public abstract RecordColumn<R, Short> getWarehouseIdColumn();
	
	public UserRecordIndex<R> maintainingComputerDecidingIndex() {
		return distributionIndex;  
	}

}
