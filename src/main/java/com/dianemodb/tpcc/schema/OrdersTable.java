package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dianemodb.ServerComputerId;
import com.dianemodb.h2impl.GroupLevelBasedIdNarrowingRule;
import com.dianemodb.h2impl.IntegerRangeBasedIdNarrowingRule;
import com.dianemodb.h2impl.RangeBasedDistributedIndex;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
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
	
	public static final String ORDER_ID_COLUMN_NAME = "o_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "o_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "o_w_id";
	public static final String CUSTOMER_ID_COLUMN_NAME = "o_c_id";
	public static final String ENTRY_DATE_COLUMN_NAME = "o_entry_d";
	public static final String CARRIER_ID_COLUMN_NAME = "o_carrier_id";
	public static final String ORDER_LINE_COUNT_COLUMN_NAME = "o_ol_cnt";	
	public static final String ALL_LOCAL_COLUMN_NAME = "o_all_local";

	public static final RecordColumn<Orders, Integer> ORDER_ID_COLUMN = 
			new RecordColumn<>(
				new IntColumn(ORDER_ID_COLUMN_NAME), 
				Orders::getOrderId, 
				Orders::setOrderId
			);

	public static final RecordColumn<Orders, Short> DISTRICT_ID_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(DISTRICT_ID_COLUMN_NAME), 
					Orders::getDistrictId, 
					Orders::setDistrictId
			);
	
	public static final RecordColumn<Orders, Short> WAREHOUSE_ID_COLUMN =
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					Orders::getWarehouseId, 
					Orders::setWarehouseId
			);

	public static final RecordColumn<Orders, Integer> CUSTOMER_ID_COLUMN = 
			new RecordColumn<>(
					new IntColumn(CUSTOMER_ID_COLUMN_NAME), 
					Orders::getCustomerId, 
					Orders::setCustomerId
			);
	
	private static final List<RecordColumn<Orders, ?>> COLUMNS = 
			List.of(
				ORDER_ID_COLUMN,
				DISTRICT_ID_COLUMN,
				WAREHOUSE_ID_COLUMN,
				CUSTOMER_ID_COLUMN,
				new RecordColumn<>(new TimestampColumn(ENTRY_DATE_COLUMN_NAME), Orders::getEntryDate, Orders::setEntryDate),
				new RecordColumn<>(new ShortColumn(CARRIER_ID_COLUMN_NAME), Orders::getCarrierId, Orders::setCarrierId),
				new RecordColumn<>(new ShortColumn(ORDER_LINE_COUNT_COLUMN_NAME), Orders::getOrderLineCount, Orders::setOrderLineCount),
				new RecordColumn<>(new ShortColumn(ALL_LOCAL_COLUMN_NAME), Orders::getAllLocal, Orders::setAllLocal)
			);

	public static final List<RecordColumn<Orders, ?>> ID_INDEX_COLUMNS = 
			List.of(
				DISTRICT_ID_COLUMN, 
				WAREHOUSE_ID_COLUMN, 
				ORDER_ID_COLUMN
			);

	private final List<RecordColumn<Orders, ?>> columns;
	
	private final DistributedIndex<Orders> compositeIndex;
	
	public OrdersTable(List<ServerComputerId> servers) {
		super(ID, TABLE_NAME);
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		this.compositeIndex = 				
				new RangeBasedDistributedIndex<>(
						servers,
						this, 
						List.of(WAREHOUSE_ID_COLUMN, DISTRICT_ID_COLUMN, CUSTOMER_ID_COLUMN),
						Map.of(
							WAREHOUSE_ID_COLUMN, new GroupLevelBasedIdNarrowingRule(1),
							DISTRICT_ID_COLUMN, new GroupLevelBasedIdNarrowingRule(1),
							// terminating
							CUSTOMER_ID_COLUMN, new IntegerRangeBasedIdNarrowingRule(1)
						)
				);
	}

	@Override
	public Orders newInstance(TransactionId txId, RecordId recordId) {
		return new Orders(txId, recordId);
	}

	@Override
	public Class<Orders> entityClass() {
		return Orders.class;
	}
	
	public DistributedIndex<Orders> getCompositeIndex() {
		return compositeIndex;
	}
	
	@Override
	protected List<RecordColumn<Orders, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<Orders>> indices() {
		return List.of(compositeIndex);
	}

	@Override
	protected DistributedIndex<Orders> getMaintainingComputerDecidingIndex() {
		return compositeIndex;
	}
}
