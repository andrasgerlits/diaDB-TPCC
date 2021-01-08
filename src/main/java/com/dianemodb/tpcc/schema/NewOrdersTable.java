package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import com.dianemodb.Topology;
import com.dianemodb.h2impl.IndexColumnDefinition;
import com.dianemodb.h2impl.RangeBasedDistributedIndex;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.ByteColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.TimestampColumn;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.tpcc.entity.NewOrders;

public class NewOrdersTable extends WarehouseBasedTable<NewOrders>{

	public static final UserRecordTableId ID = new UserRecordTableId(NEW_ORDERS_TABLE_ID);
	
	public static final String TABLE_NAME = "new_orders";
	
	public static final String ORDER_ID_COLUMN_NAME = "o_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "o_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "o_w_id";
	public static final String CUSTOMER_ID_COLUMN_NAME = "o_c_id";
	public static final String ENTRY_TIME_COLUMN_NAME = "o_entry_d";
	public static final String CARRIER_ID_COLUMN_NAME = "o_carrier_id";
	public static final String LINE_COLUMN_NAME = "o_ol_cnt";
	public static final String ALL_LOCAL_COLUMN_NAME = "o_all_local";
	
	public static final RecordColumn<NewOrders, TransactionId> TX_ID_COLUMN = TX_ID();
	public static final RecordColumn<NewOrders, Long> RECORD_ID_COLUMN = RECORD_ID();

	public static final RecordColumn<NewOrders, Byte> DISTRICT_ID_COLUMN = 				
			new RecordColumn<>(
					new ByteColumn(DISTRICT_ID_COLUMN_NAME), 
					NewOrders::getDistrictId, 
					NewOrders::setDistrictId
			);
	
	public static final RecordColumn<NewOrders, Short> WAREHOUSE_ID_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					NewOrders::getWarehouseId, 
					NewOrders::setWarehouseId
			);
	
	public static final RecordColumn<NewOrders, Short> CARRIER_ID_COLUMN =
			new RecordColumn<>(
					new ShortColumn(CARRIER_ID_COLUMN_NAME), 
					NewOrders::getCarrierId, 
					NewOrders::setCarrierId
			);
	
	public static final RecordColumn<NewOrders, Integer> ORDER_ID_COLUMN = 
			new RecordColumn<>(
					new IntColumn(ORDER_ID_COLUMN_NAME), 
					NewOrders::getOrderId, 
					NewOrders::setOrderId
			);
	
	public static final List<RecordColumn<NewOrders, ?>> COLUMNS = 
			List.of(
				ORDER_ID_COLUMN,
				DISTRICT_ID_COLUMN,
				WAREHOUSE_ID_COLUMN,
				new RecordColumn<>(
						new IntColumn(CUSTOMER_ID_COLUMN_NAME), 
						NewOrders::getCustomerId, 
						NewOrders::setCustomerId
				),
				new RecordColumn<>(
						new TimestampColumn(ENTRY_TIME_COLUMN_NAME), 
						NewOrders::getEntryTime, 
						NewOrders::setEntryTime
				),
				CARRIER_ID_COLUMN,
				new RecordColumn<>(
						new ShortColumn(LINE_COLUMN_NAME), 
						NewOrders::getLine, 
						NewOrders::setLine
				),
				new RecordColumn<>(
						new ShortColumn(ALL_LOCAL_COLUMN_NAME), 
						NewOrders::getAllLocal, 
						NewOrders::setAllLocal
				)
			);

	private final LinkedList<RecordColumn<NewOrders, ?>> columns;

	private final Collection<UserRecordIndex<NewOrders>> indices;
	
	private final RangeBasedDistributedIndex<NewOrders> compositeIndex;
	
	public NewOrdersTable(Topology servers) {
		super(ID, TABLE_NAME, Caching.CACHED, servers);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		// we always look for the lowest order-id for new-orders 
		compositeIndex = 
				new RangeBasedDistributedIndex<>(
						servers,
						this, 
						List.of(
							warehouseIndexColumnDefinition, 
							new IndexColumnDefinition<>(DISTRICT_ID_COLUMN), 
							new IndexColumnDefinition<>(ORDER_ID_COLUMN)
						)
				);

		this.indices = List.of(compositeIndex);
	}
	
	public RangeBasedDistributedIndex<NewOrders> getCompositeIndex() {
		return compositeIndex;
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
	protected List<RecordColumn<NewOrders, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<UserRecordIndex<NewOrders>> indices() {
		return indices;
	}

	@Override
	public RecordColumn<NewOrders, Short> getWarehouseIdColumn() {
		return WAREHOUSE_ID_COLUMN;
	}
}
