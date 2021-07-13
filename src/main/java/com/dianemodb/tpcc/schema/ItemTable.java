package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.Topology;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.h2impl.H2RangeBasedDistributedIndex;
import com.dianemodb.id.IndexTableId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.column.BigDecimalColumn;
import com.dianemodb.metaschema.column.IntColumn;
import com.dianemodb.metaschema.column.RecordColumn;
import com.dianemodb.metaschema.column.ShortColumn;
import com.dianemodb.metaschema.column.StringColumn;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.SingleIndexBasedQueryPlan;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.query.IndexColumnDefinition;
import com.dianemodb.query.RangeBasedDistributedIndex;
import com.dianemodb.tpcc.entity.Item;

public class ItemTable extends TpccBaseTable<Item> {

	public static final UserRecordTableId ID = new UserRecordTableId(ITEM_TABLE_ID);
	
	public static final String TABLE_NAME = "item";
	
	public static final String ID_COLUMN_NAME = "i_id";
	public static final String IM_ID_COLUMN_NAME = "i_im_id";
	public static final String NAME_COLUMN_NAME = "i_name";
	public static final String PRICE_COLUMN_NAME = "i_price";
	public static final String DATA_COLUMN_NAME = "i_data";
	private static final String DISTRIBUTION_ID_COLUMN_NAME = "dist_id";
	
	public static final RecordColumn<Item, Integer> ID_COLUMN = 
			new RecordColumn<>(new IntColumn(ID_COLUMN_NAME), Item::getItemId, Item::setItemId);
	
	private static final RecordColumn<Item, Short> DISTRIBUTION_ID_COLUMN = 
			new RecordColumn<>(new ShortColumn(DISTRIBUTION_ID_COLUMN_NAME), Item::getDistId, Item::setDistId);
	
	private static final List<RecordColumn<Item, ?>> COLUMNS = 
			List.of(
				ID_COLUMN,
				DISTRIBUTION_ID_COLUMN,
				new RecordColumn<>(new IntColumn(IM_ID_COLUMN_NAME), Item::getIm, Item::setIm),
				new RecordColumn<>(new StringColumn(NAME_COLUMN_NAME, 24), Item::getName, Item::setName),
				new RecordColumn<>(new BigDecimalColumn(PRICE_COLUMN_NAME, 5, 2), Item::getPrice, Item::setPrice),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME, 50), Item::getData, Item::setData)
			);

	private final List<RecordColumn<Item, ?>> columns;

	private final Collection<UserRecordIndex<Item>> indices;

	private final RangeBasedDistributedIndex<Item> idIndex;
	
	public ItemTable(Topology servers) {
		super(ID, TABLE_NAME, Caching.CACHED, servers);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		this.idIndex = 			
			new H2RangeBasedDistributedIndex<>(
					new IndexTableId(0, ID),
					servers, 
					this, 
					List.of(
						// follows the same distribution as warehouses, but on its own ID
						new IndexColumnDefinition<>(
								DISTRIBUTION_ID_COLUMN, 
								WarehouseTable.getWarehouseDistributionRule()
						), 
						new IndexColumnDefinition<>(ID_COLUMN)
				)
			);

		this.indices = List.of(idIndex);
	}

	@Override
	public Item newInstance(TransactionId txId, RecordId recordId) {
		return new Item(txId, recordId);
	}
	
	public short getDistributionIndexForServer(ServerComputerId id) {
		int index = getRecordMaintainingComputers().indexOf(id);
		
		assert index != -1 : getRecordMaintainingComputers() + " " + id;

		assert index <= Short.MAX_VALUE : getRecordMaintainingComputers() + "\n" + id;
		
		return (short) index;
	}

	@Override
	public Class<Item> entityClass() {
		return Item.class;
	}
	
	public RangeBasedDistributedIndex<Item> getIdIndex() {
		return idIndex;
	}

	@Override
	protected List<RecordColumn<Item, ?>> columns() {
		return columns;
	}

	@Override
	protected Collection<UserRecordIndex<Item>> indices() {
		return indices;
	}

	@Override
	protected UserRecordIndex<Item> maintainingComputerDecidingIndex() {
		return idIndex;
	}
}
