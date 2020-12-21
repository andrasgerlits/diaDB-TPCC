package com.dianemodb.tpcc.schema;

import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.UserRecord;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.schema.UserRecordTable;

public abstract class TpccBaseTable<R extends UserRecord> extends UserRecordTable<R> {
	
	protected static final long CUSTOMER_TABLE_ID = 0L;
	protected static final long DISTRICT_TABLE_ID = 1L;
	protected static final long HISTORY_TABLE_ID = 2L;
	protected static final long ITEM_TABLE_ID = 3L;
	protected static final long NEW_ORDERS_TABLE_ID = 4L;
	protected static final long ORDERS_LINE_TABLE_ID = 5L;
	protected static final long ORDERS_TABLE_ID = 6L;
	protected static final long STOCK_TABLE_ID = 7L;	
	protected static final long WAREHOUSE_TABLE_ID = 8L;

	private final List<RecordColumn<R, ?>> columns;
	private final RecordColumn<R, TransactionId> txIdColumn;
	private final RecordColumn<R, RecordId> recordIdColumn;
	
	public TpccBaseTable(UserRecordTableId tableId, String name) {
		this(tableId, name, Caching.CACHED);
	}
	
	public TpccBaseTable(UserRecordTableId tableId, String name, Caching caching) {
		super(tableId, name, caching);
		
		this.txIdColumn = TX_ID();
		this.recordIdColumn = RECORD_ID();

		this.columns = List.of(txIdColumn, recordIdColumn);
	}
	
	@Override
	protected List<RecordColumn<R, ?>> columns() {
		return columns;
	}

	@Override
	public RecordColumn<R, RecordId> getRecordIdColumn() {
		return recordIdColumn;
	}
	
	protected abstract DistributedIndex<R> getMaintainingComputerDecidingIndex();
	
	@Override
	public final ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			R thing
	) {
		return getMaintainingComputerDecidingIndex().getSingleMaintainingComputer(thing);
	}

	@Override
	public RecordColumn<R, TransactionId> getTransactionIdColumn() {
		return txIdColumn;
	}

}
