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
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.tpcc.entity.Stock;

public class StockTable extends TpccBaseTable<Stock> {

	public static final UserRecordTableId ID = new UserRecordTableId(STOCK_TABLE_ID);
	
	public static final String TABLE_NAME = "item";
	
	public static final String ID_COLUMN_NAME = "s_i_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "";
	public static final String QUANTITY_COLUMN_NAME = "";
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
	
	
	private static final List<RecordColumn<Stock, ?>> COLUMNS = 
			List.of(
				new RecordColumn<>(new IntColumn(ID_COLUMN_NAME), Stock::getPublicId),
				new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), Stock::getWarehouseId),
				new RecordColumn<>(new ShortColumn(QUANTITY_COLUMN_NAME), Stock::getQuantity),
				new RecordColumn<>(new StringColumn(DIST_1_COLUMN_NAME), Stock::getDist1),
				new RecordColumn<>(new StringColumn(DIST_2_COLUMN_NAME), Stock::getDist2),
				new RecordColumn<>(new StringColumn(DIST_3_COLUMN_NAME), Stock::getDist3),
				new RecordColumn<>(new StringColumn(DIST_4_COLUMN_NAME), Stock::getDist4),
				new RecordColumn<>(new StringColumn(DIST_5_COLUMN_NAME), Stock::getDist5),
				new RecordColumn<>(new StringColumn(DIST_6_COLUMN_NAME), Stock::getDist6),
				new RecordColumn<>(new StringColumn(DIST_7_COLUMN_NAME), Stock::getDist7),
				new RecordColumn<>(new StringColumn(DIST_8_COLUMN_NAME), Stock::getDist8),
				new RecordColumn<>(new StringColumn(DIST_9_COLUMN_NAME), Stock::getDist9),
				new RecordColumn<>(new StringColumn(DIST_10_COLUMN_NAME), Stock::getDist10),
				new RecordColumn<>(new StringColumn(YTD_COLUMN_NAME), Stock::getData),
				new RecordColumn<>(new StringColumn(ORDER_COLUMN_NAME), Stock::getData),
				new RecordColumn<>(new StringColumn(REMOTE_COLUMN_NAME), Stock::getData),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME), Stock::getData)
			);

	private final LinkedList<RecordColumn<Stock, ?>> columns;
	private final Collection<DistributedIndex<Stock, ?>> indices; 

	public StockTable() {
		super(ID, TABLE_NAME);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		this.indices = List.of();
	}


	@Override
	public Stock newInstance(TransactionId txId, RecordId recordId) {
		return new Stock(txId, recordId);
	}


	@Override
	public Class<Stock> entityClass() {
		return Stock.class;
	}


	@Override
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			Stock thing
	) {
		return null;
	}


	@Override
	protected List<RecordColumn<Stock, ?>> columns() {
		return columns;
	}


	@Override
	protected Collection<DistributedIndex<Stock, ?>> indices() {
		return indices;
	}


}