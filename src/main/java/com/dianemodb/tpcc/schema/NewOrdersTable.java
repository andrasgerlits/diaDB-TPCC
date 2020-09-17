package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.TimestampColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.tpcc.entity.NewOrders;

public class NewOrdersTable extends TpccBaseTable<NewOrders>{

	public static final UserRecordTableId ID = new UserRecordTableId(NEW_ORDERS_TABLE_ID);
	
	public static final String TABLE_NAME = "orders";
	
	public static final String PUBLIC_ID_COLUMN_NAME = "o_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "o_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "o_w_id";
	public static final String CUSTOMER_ID_COLUMN_NAME = "o_c_id";
	public static final String ENTRY_TIME_COLUMN_NAME = "o_entry_d";
	public static final String CARRIER_ID_COLUMN_NAME = "o_carrier_id";
	public static final String LINE_COLUMN_NAME = "o_ol_cnt";
	public static final String ALL_LOCAL_COLUMN_NAME = "o_all_local";
	
	public static final RecordColumn<NewOrders, TransactionId> TX_ID_COLUMN = TX_ID();
	public static final RecordColumn<NewOrders, RecordId> RECORD_ID_COLUMN = RECORD_ID();
	
	public static final List<RecordColumn<NewOrders, ?>> COLUMNS = 
			List.of(
				new RecordColumn<>(new IntColumn(PUBLIC_ID_COLUMN_NAME), NewOrders::getPublicId),
				new RecordColumn<>(new ShortColumn(DISTRICT_ID_COLUMN_NAME), NewOrders::getDistrictId),
				new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), NewOrders::getWarehouseId),
				new RecordColumn<>(new IntColumn(CUSTOMER_ID_COLUMN_NAME), NewOrders::getCustomerId),
				new RecordColumn<>(new TimestampColumn(ENTRY_TIME_COLUMN_NAME), NewOrders::getEntryTime),
				new RecordColumn<>(new ShortColumn(CARRIER_ID_COLUMN_NAME), NewOrders::getCarrierId),
				new RecordColumn<>(new ShortColumn(LINE_COLUMN_NAME), NewOrders::getLine),
				new RecordColumn<>(new ShortColumn(ALL_LOCAL_COLUMN_NAME), NewOrders::getAllLocal)
			);

	private final LinkedList<RecordColumn<NewOrders, ?>> columns;
	
	public NewOrdersTable(Collection<ServerComputerId> servers) {
		super(ID, TABLE_NAME);
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
	}

	@Override
	public NewOrders newInstance(TransactionId txId, RecordId recordId) {
		return new NewOrders(txId, recordId);
	}

	@Override
	public Class<NewOrders> entityClass() {
		return NewOrders.class;
	}

	@Override
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			NewOrders thing
	) {
		return null;
	}

	@Override
	protected List<RecordColumn<NewOrders, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<NewOrders, ?>> indices() {
		return List.of();
	}
}
