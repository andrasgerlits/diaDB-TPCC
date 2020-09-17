package com.dianemodb.tpcc.schema;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.TimestampColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.tpcc.entity.History;
/*
h_c_id int, 
h_c_d_id tinyint, 
h_c_w_id smallint,
h_d_id tinyint,
h_w_id smallint,
h_date datetime,
h_amount decimal(6,2), 
h_data varchar(24) )
*/ 
public class HistoryTable extends TpccBaseTable<History> {
	
	public static final UserRecordTableId ID = new UserRecordTableId(HISTORY_TABLE_ID);

	public static final String TABLE_NAME = "history";
	
	private static final String CUSTOMER_ID_COLUMN_NAME = "h_c_id";
	private static final String CUSTOMER_DISTRICT_ID_COLUMN_NAME = "h_c_d_id";
	private static final String CUSTOMER_WAREHOUSE_ID_COLUMN_NAME = "h_c_w_id";
	private static final String DISTRICT_ID_COLUMN_NAME= "h_d_id";
	private static final String WAREHOUSE_ID_COLUMN_NAME = "h_w_id";
	private static final String DATE_COLUMN_NAME = "h_date";
	private static final String AMOUNT_COLUMN_NAME = "h_amount";
	private static final String DATA_COLUMN_NAME = "h_data";

	private static final List<RecordColumn<History, ?>> COLUMNS = 
			List.of(
				new RecordColumn<>(new IntColumn(CUSTOMER_ID_COLUMN_NAME), History::getCustomerId),
				new RecordColumn<>(new ShortColumn(CUSTOMER_DISTRICT_ID_COLUMN_NAME), History::getCustomerDistrictId),
				new RecordColumn<>(new ShortColumn(CUSTOMER_WAREHOUSE_ID_COLUMN_NAME), History::getCustomerWarehouseId),
				new RecordColumn<>(new ShortColumn(DISTRICT_ID_COLUMN_NAME), History::getDistrictId),
				new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), History::getWarehouseId),
				new RecordColumn<>(new TimestampColumn(DATE_COLUMN_NAME), History::getDate),
				new RecordColumn<>(new BigDecimalColumn(AMOUNT_COLUMN_NAME, 6, 2), History::getAmount),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME), History::getData)
			);

	private final List<RecordColumn<History, ?>> columns;

	private final Collection<DistributedIndex<History, ?>> indices;
	
	
	public HistoryTable(Collection<ServerComputerId> servers) {
		super(ID, TABLE_NAME);
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		this.indices = List.of();
	}

	@Override
	public History newInstance(TransactionId txId, RecordId recordId) {
		return new History(txId, recordId);
	}

	@Override
	public Class<History> entityClass() {
		return History.class;
	}

	@Override
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			History thing
	) {
		return null;
	}
	
	@Override
	protected List<RecordColumn<History, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<History, ?>> indices() {
		return indices;
	}
}
