package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dianemodb.Topology;
import com.dianemodb.h2impl.H2RangeBasedDistributedIndex;
import com.dianemodb.id.IndexTableId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.column.IntColumn;
import com.dianemodb.metaschema.column.LongColumn;
import com.dianemodb.metaschema.column.RecordColumn;
import com.dianemodb.metaschema.column.ShortColumn;
import com.dianemodb.metaschema.column.StringColumn;
import com.dianemodb.metaschema.distributed.ServerComputerIdNarrowingRule;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.query.IndexColumnDefinition;
import com.dianemodb.query.NullRule;
import com.dianemodb.query.RangeBasedDistributedIndex;
import com.dianemodb.tpcc.entity.Stock;

public class StockTable extends WarehouseBasedTable<Stock> {

	public static final UserRecordTableId ID = new UserRecordTableId(STOCK_TABLE_ID);
	
	public static final String TABLE_NAME = "stock";
	
	public static final String ITEM_ID_COLUMN_NAME = "s_i_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "s_w_id";
	public static final String QUANTITY_COLUMN_NAME = "s_quantity";
	public static final String DIST_1_COLUMN_NAME = "s_dist_01";
	public static final String DIST_2_COLUMN_NAME = "s_dist_02";
	public static final String DIST_3_COLUMN_NAME = "s_dist_03";
	public static final String DIST_4_COLUMN_NAME = "s_dist_04";
	public static final String DIST_5_COLUMN_NAME = "s_dist_05";
	public static final String DIST_6_COLUMN_NAME = "s_dist_06";
	public static final String DIST_7_COLUMN_NAME = "s_dist_07";
	public static final String DIST_8_COLUMN_NAME = "s_dist_08";
	public static final String DIST_9_COLUMN_NAME = "s_dist_09";
	public static final String DIST_10_COLUMN_NAME = "s_dist_10";
	public static final String YTD_COLUMN_NAME = "s_ytd";
	public static final String ORDER_COLUMN_NAME = "s_order_cnt";
	public static final String REMOTE_COLUMN_NAME = "s_remote_cnt";
	public static final String DATA_COLUMN_NAME = "s_data";

	public static final RecordColumn<Stock, Integer> ITEM_ID_COLUMN = 
			new RecordColumn<>(
					new IntColumn(ITEM_ID_COLUMN_NAME), 
					Stock::getItemId,
					Stock::setItemId
			);

	public static final RecordColumn<Stock, Short> WAREHOUSE_ID_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					Stock::getWarehouseId,
					Stock::setWarehouseId
			);
	
	private static final List<RecordColumn<Stock, ?>> COLUMNS = 
			List.of(
				ITEM_ID_COLUMN,
				WAREHOUSE_ID_COLUMN,
				new RecordColumn<>(new ShortColumn(QUANTITY_COLUMN_NAME), Stock::getQuantity, Stock::setQuantity),
				new RecordColumn<>(new StringColumn(DIST_1_COLUMN_NAME, 24), Stock::getDist1, Stock::setDist1),
				new RecordColumn<>(new StringColumn(DIST_2_COLUMN_NAME, 24), Stock::getDist2, Stock::setDist2),
				new RecordColumn<>(new StringColumn(DIST_3_COLUMN_NAME, 24), Stock::getDist3, Stock::setDist3),
				new RecordColumn<>(new StringColumn(DIST_4_COLUMN_NAME, 24), Stock::getDist4, Stock::setDist4),
				new RecordColumn<>(new StringColumn(DIST_5_COLUMN_NAME, 24), Stock::getDist5, Stock::setDist5),
				new RecordColumn<>(new StringColumn(DIST_6_COLUMN_NAME, 24), Stock::getDist6, Stock::setDist6),
				new RecordColumn<>(new StringColumn(DIST_7_COLUMN_NAME, 24), Stock::getDist7, Stock::setDist7),
				new RecordColumn<>(new StringColumn(DIST_8_COLUMN_NAME, 24), Stock::getDist8, Stock::setDist8),
				new RecordColumn<>(new StringColumn(DIST_9_COLUMN_NAME, 24), Stock::getDist9, Stock::setDist9),
				new RecordColumn<>(new StringColumn(DIST_10_COLUMN_NAME, 24), Stock::getDist10, Stock::setDist10),
				new RecordColumn<>(new LongColumn(YTD_COLUMN_NAME), Stock::getYtd, Stock::setYtd),
				new RecordColumn<>(new ShortColumn(ORDER_COLUMN_NAME), Stock::getOrderCnt, Stock::setOrderCnt),
				new RecordColumn<>(new ShortColumn(REMOTE_COLUMN_NAME), Stock::getRemoteCnt, Stock::setRemoteCnt),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME, 50), Stock::getData, Stock::setData)
			);

	private final LinkedList<RecordColumn<Stock, ?>> columns;
	private final Collection<UserRecordIndex<Stock>> indices;

	private final RangeBasedDistributedIndex<Stock> itemWarehouseIndex;
 
	public StockTable(Topology servers) {
		super(ID, TABLE_NAME, servers);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		// stock lives on the same computer as the warehouse
		Map<RecordColumn<Stock,?>, ServerComputerIdNarrowingRule> indexRuleMap = new HashMap<>();
		indexRuleMap.put(WAREHOUSE_ID_COLUMN, WarehouseTable.getWarehouseDistributionRule());
		indexRuleMap.put(ITEM_ID_COLUMN, NullRule.INSTANCE);
		
		itemWarehouseIndex = 				
				new H2RangeBasedDistributedIndex<>(
						new IndexTableId(0, ID),
						servers,
						this, 
						List.of(
							warehouseIndexColumnDefinition, 
							new IndexColumnDefinition<>(ITEM_ID_COLUMN)
						)
				);
		
		this.indices = List.of(itemWarehouseIndex);
	}

	@Override
	public Stock newInstance(TransactionId txId, RecordId recordId) {
		return new Stock(txId, recordId);
	}

	@Override
	public Class<Stock> entityClass() {
		return Stock.class;
	}

	public RangeBasedDistributedIndex<Stock> getItemWarehouseIndex() {
		return itemWarehouseIndex;
	}

	@Override
	protected List<RecordColumn<Stock, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<UserRecordIndex<Stock>> indices() {
		return indices;
	}

	@Override
	public RecordColumn<Stock, Short> getWarehouseIdColumn() {
		return WAREHOUSE_ID_COLUMN;
	}
}
