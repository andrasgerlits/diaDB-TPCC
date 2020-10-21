package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dianemodb.Topology;
import com.dianemodb.h2impl.NullRule;
import com.dianemodb.h2impl.RangeBasedDistributedIndex;
import com.dianemodb.h2impl.ServerComputerIdNarrowingRule;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.ByteColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.TimestampColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.tpcc.entity.OrderLine;

public class OrderLineTable extends TpccBaseTable<OrderLine> {

	public static final UserRecordTableId ID = new UserRecordTableId(ORDERS_LINE_TABLE_ID);
	
	public static final String TABLE_NAME = "order_line";

	public static final String ORDER_ID_COLUMN_NAME = "ol_o_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "ol_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "ol_w_id";
	public static final String NUMBER_COLUMN_NAME = "ol_number";
	public static final String ITEM_ID_COLUMN_NAME = "ol_i_id";
	public static final String SUPPLY_WAREHOUSE_COLUMN_NAME = "ol_supply_w_id";
	public static final String DELIVERY_DATE_COLUMN_NAME = "ol_delivery_d";
	public static final String QUANTITIY_COLUMN_NAME = "ol_quantity";
	public static final String AMOUNT_COLUMN_NAME = "ol_amount";
	public static final String DIST_INFO_COLUMN_NAME = "ol_dist_info";
	
	public static final RecordColumn<OrderLine, Integer> ORDER_ID_COLUMN =
			new RecordColumn<>(
					new IntColumn(ORDER_ID_COLUMN_NAME), 
					OrderLine::getOrderId, 
					OrderLine::setOrderId
			);
	
	public static final RecordColumn<OrderLine, Byte> DISTRICT_ID_COLUMN = 
			new RecordColumn<>(
					new ByteColumn(DISTRICT_ID_COLUMN_NAME), 
					OrderLine::getDistrictId, 
					OrderLine::setDistrictId
			);
	
	public static final RecordColumn<OrderLine, Short> WAREHOUSE_ID_COLUMN =
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					OrderLine::getWarehouseId, 
					OrderLine::setWarehouseId
			);

	private static final List<RecordColumn<OrderLine, ?>> COLUMNS = 
			List.of(
				ORDER_ID_COLUMN,
				DISTRICT_ID_COLUMN,
				WAREHOUSE_ID_COLUMN,
				new RecordColumn<>(new ShortColumn(NUMBER_COLUMN_NAME), OrderLine::getLineNumber, OrderLine::setLineNumber),
				new RecordColumn<>(new ShortColumn(ITEM_ID_COLUMN_NAME), OrderLine::getItemId, OrderLine::setItemId),
				new RecordColumn<>(new ShortColumn(SUPPLY_WAREHOUSE_COLUMN_NAME), OrderLine::getSupplyWarehouseId, OrderLine::setSupplyWarehouseId),
				new RecordColumn<>(new TimestampColumn(DELIVERY_DATE_COLUMN_NAME), OrderLine::getDeliveryDate, OrderLine::setDeliveryDate),
				new RecordColumn<>(new ShortColumn(QUANTITIY_COLUMN_NAME), OrderLine::getQuantity, OrderLine::setQuantity),
				new RecordColumn<>(new BigDecimalColumn(AMOUNT_COLUMN_NAME, 6, 2), OrderLine::getAmount, OrderLine::setAmount),
				new RecordColumn<>(new StringColumn(DIST_INFO_COLUMN_NAME, 24), OrderLine::getDistInfo, OrderLine::setDistInfo)
			);
	
	private final List<RecordColumn<OrderLine, ?>> columns;
	private final Collection<DistributedIndex<OrderLine>> indices;
	
	private final DistributedIndex<OrderLine> orderIdRangeIndex;
	
	public OrderLineTable(Topology servers) {
		super(ID, TABLE_NAME);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		Map<RecordColumn<OrderLine,?>, ServerComputerIdNarrowingRule> indexRuleMap = 
				DistrictTable.getDistrictBasedRoundRobinRules(
						WAREHOUSE_ID_COLUMN, 
						DISTRICT_ID_COLUMN
				);
		
		indexRuleMap.put(ORDER_ID_COLUMN, NullRule.INSTANCE);
		
		orderIdRangeIndex = 
				new RangeBasedDistributedIndex<>(
						servers,
						this, 
						List.of(WAREHOUSE_ID_COLUMN, DISTRICT_ID_COLUMN, ORDER_ID_COLUMN),
						indexRuleMap
				);

		this.indices = List.of(orderIdRangeIndex);
	}

	@Override
	public OrderLine newInstance(TransactionId txId, RecordId recordId) {
		return new OrderLine(txId, recordId);
	}

	public DistributedIndex<OrderLine> getOrderIdRangeIndex() {
		return orderIdRangeIndex;
	}

	@Override
	public Class<OrderLine> entityClass() {
		return OrderLine.class;
	}

	@Override
	protected List<RecordColumn<OrderLine, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<OrderLine>> indices() {
		return indices;
	}

	@Override
	protected DistributedIndex<OrderLine> getMaintainingComputerDecidingIndex() {
		return orderIdRangeIndex;
	}

}
