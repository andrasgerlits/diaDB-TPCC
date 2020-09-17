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
import com.dianemodb.tpcc.entity.Orders;

/*
 create table orders (
o_id int not null, 
o_d_id tinyint not null, 
 smallint not null,
 int,
 datetime,
 tinyint,
 tinyint, 
 tinyint, 
 */
public class OrdersTable extends TpccBaseTable<Orders> {

	public static final UserRecordTableId ID = new UserRecordTableId(ORDERS_TABLE_ID);
	
	public static final String TABLE_NAME = "orders";
	
	public static final String ID_COLUMN_NAME = "o_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "o_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "o_w_id";
	public static final String CUSTOMER_ID_COLUMN_NAME = "o_c_id";
	public static final String ENTRY_DATE_COLUMN_NAME = "o_entry_d";
	public static final String CARRIER_ID_COLUMN_NAME = "o_carrier_id";
	public static final String ORDER_LINE_COUNT_COLUMN_NAME = "o_ol_cnt";	
	public static final String ALL_LOCAL_COLUMN_NAME = "o_all_local";
	
	private static final List<RecordColumn<Orders, ?>> COLUMNS = 
			List.of(
				new RecordColumn<>(new IntColumn(ID_COLUMN_NAME), Orders::getPublicId),
				new RecordColumn<>(new ShortColumn(DISTRICT_ID_COLUMN_NAME), Orders::getDistrictId),
				new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), Orders::getWarehouseId),
				new RecordColumn<>(new IntColumn(CUSTOMER_ID_COLUMN_NAME), Orders::getCustomerId),
				new RecordColumn<>(new TimestampColumn(ENTRY_DATE_COLUMN_NAME), Orders::getEntryDate),
				new RecordColumn<>(new ShortColumn(CARRIER_ID_COLUMN_NAME), Orders::getCarrierId),
				new RecordColumn<>(new ShortColumn(ORDER_LINE_COUNT_COLUMN_NAME), Orders::getOrderLineCount),
				new RecordColumn<>(new ShortColumn(ALL_LOCAL_COLUMN_NAME), Orders::getAllLocal)
			);

	private final List<RecordColumn<Orders, ?>> columns;
	private final List<DistributedIndex<Orders, ?>> indices;
	
	public OrdersTable() {
		super(ID, TABLE_NAME);
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);

		this.indices = List.of();
	}

	@Override
	public Orders newInstance(TransactionId txId, RecordId recordId) {
		return new Orders(txId, recordId);
	}

	@Override
	public Class<Orders> entityClass() {
		return Orders.class;
	}

	@Override
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			Orders thing
	) {
		return null;
	}

	@Override
	protected List<RecordColumn<Orders, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<Orders, ?>> indices() {
		return indices;
	}
}
