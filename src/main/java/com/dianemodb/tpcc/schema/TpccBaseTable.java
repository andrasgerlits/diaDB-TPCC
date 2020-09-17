package com.dianemodb.tpcc.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import com.dianemodb.UserRecord;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;
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
		super(tableId, name);
		
		this.txIdColumn = TX_ID();
		this.recordIdColumn = RECORD_ID();

		this.columns = List.of(txIdColumn, recordIdColumn);
	}
	
	protected R setFieldsFromResultSet(ResultSet rs) throws SQLException {
		RecordId recordId = RecordId.valueOf(rs.getString(RECORD_ID_COLUMN_NAME));
		TransactionId txId = TransactionId.valueOf(rs.getString(TX_ID_COLUMN_NAME));
		
		return newInstance(txId, recordId);
	}

	@Override
	protected List<RecordColumn<R, ?>> columns() {
		return columns;
	}

	@Override
	public RecordColumn<R, RecordId> getRecordIdColumn() {
		return recordIdColumn;
	}

	@Override
	public RecordColumn<R, TransactionId> getTransactionIdColumn() {
		return txIdColumn;
	}

}
