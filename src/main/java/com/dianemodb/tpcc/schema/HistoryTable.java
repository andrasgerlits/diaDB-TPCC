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

	private static final RecordColumn<History, Integer> CUSTOMER_ID_COLUMN = 
				new RecordColumn<>(
					new IntColumn(CUSTOMER_ID_COLUMN_NAME), 
					History::getCustomerId, 
					History::setCustomerId
				);

	private static final RecordColumn<History, Byte> CUSTOMER_DISTRICT_ID_COLUMN = 				
			new RecordColumn<>(
				new ByteColumn(CUSTOMER_DISTRICT_ID_COLUMN_NAME), 
				History::getCustomerDistrictId,
				History::setCustomerDistrictId
			);

	private static final RecordColumn<History, Short> CUSTOMER_WAREHOUSE_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(CUSTOMER_WAREHOUSE_ID_COLUMN_NAME), 
					History::getCustomerWarehouseId,
					History::setCustomerWarehouseId
			);

	private static final RecordColumn<History, Byte> DISTRICT_ID_COLUMN = 
			new RecordColumn<>(
					new ByteColumn(DISTRICT_ID_COLUMN_NAME), 
					History::getDistrictId,
					History::setDistrictId
			);

	private static final RecordColumn<History, Short> WAREHOUSE_ID_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					History::getWarehouseId,
					History::setWarehouseId
			);
	
	

	private static final List<RecordColumn<History, ?>> COLUMNS = 
			List.of(
				CUSTOMER_ID_COLUMN,
				CUSTOMER_DISTRICT_ID_COLUMN,
				CUSTOMER_WAREHOUSE_COLUMN,
				DISTRICT_ID_COLUMN,
				WAREHOUSE_ID_COLUMN,
				new RecordColumn<>(
					new TimestampColumn(DATE_COLUMN_NAME), 
					History::getDate,
					History::setDate
				),
				new RecordColumn<>(
					new BigDecimalColumn(AMOUNT_COLUMN_NAME, 6, 2), 
					History::getAmount,
					History::setAmount
				),
				new RecordColumn<>(
					new StringColumn(DATA_COLUMN_NAME), 
					History::getData,
					History::setData
				)
			);

	private final List<RecordColumn<History, ?>> columns;

	private final Collection<DistributedIndex<History>> indices;
	
	private final RangeBasedDistributedIndex<History> compositeIndex;
	
	public HistoryTable(Topology servers) {
		super(ID, TABLE_NAME);
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);

		Map<RecordColumn<History,?>, ServerComputerIdNarrowingRule> indexRuleMap = 
				DistrictTable.getDistrictBasedRoundRobinRules(
						CUSTOMER_WAREHOUSE_COLUMN, 
						CUSTOMER_DISTRICT_ID_COLUMN
				);
		
		indexRuleMap.put(CUSTOMER_ID_COLUMN, NullRule.INSTANCE);

		this.compositeIndex = 
				new RangeBasedDistributedIndex<>(
						servers,
						this, 
						List.of(
							CUSTOMER_WAREHOUSE_COLUMN, 
							CUSTOMER_DISTRICT_ID_COLUMN, 
							CUSTOMER_ID_COLUMN
						),
						indexRuleMap
				);

		this.indices = List.of(compositeIndex);
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
	protected List<RecordColumn<History, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<DistributedIndex<History>> indices() {
		return indices;
	}

	@Override
	protected DistributedIndex<History> getMaintainingComputerDecidingIndex() {
		return compositeIndex;
	}
}
