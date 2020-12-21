package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dianemodb.Topology;
import com.dianemodb.h2impl.NullRule;
import com.dianemodb.h2impl.RangeBasedDistributedIndex;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.distributed.ServerComputerIdNarrowingRule;
import com.dianemodb.tpcc.entity.Item;

public class ItemTable extends TpccBaseTable<Item> {

	public static final UserRecordTableId ID = new UserRecordTableId(ITEM_TABLE_ID);
	
	public static final String TABLE_NAME = "item";
	
	public static final String ID_COLUMN_NAME = "i_id";
	public static final String IM_ID_COLUMN_NAME = "i_im_id";
	public static final String NAME_COLUMN_NAME = "i_name";
	public static final String PRICE_COLUMN_NAME = "i_price";
	public static final String DATA_COLUMN_NAME = "i_data";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "i_wh";
	
	public static final RecordColumn<Item, Integer> ID_COLUMN = 
			new RecordColumn<>(new IntColumn(ID_COLUMN_NAME), Item::getItemId, Item::setItemId);
	
	public static final RecordColumn<Item, Short> WAREHOUSE_ID_COLUMN = 
			new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), Item::getWarehouseId, Item::setWarehouseId);

	private static final List<RecordColumn<Item, ?>> COLUMNS = 
			List.of(
				ID_COLUMN,
				new RecordColumn<>(new IntColumn(IM_ID_COLUMN_NAME), Item::getIm, Item::setIm),
				new RecordColumn<>(new StringColumn(NAME_COLUMN_NAME), Item::getName, Item::setName),
				new RecordColumn<>(new BigDecimalColumn(PRICE_COLUMN_NAME, 5, 2), Item::getPrice, Item::setPrice),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME), Item::getData, Item::setData),
				WAREHOUSE_ID_COLUMN
		);

	private final List<RecordColumn<Item, ?>> columns;

	private final Collection<DistributedIndex<Item>> indices;

	private final RangeBasedDistributedIndex<Item> idIndex;
	
	public ItemTable(Topology servers) {
		super(ID, TABLE_NAME, Caching.MEMORY);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		Map<RecordColumn<Item,?>, ServerComputerIdNarrowingRule> indexRuleMap = new HashMap<>();
		indexRuleMap.put(WAREHOUSE_ID_COLUMN, WarehouseTable.getWarehouseDistributionRule());
		indexRuleMap.put(ID_COLUMN, NullRule.INSTANCE);
		
		this.idIndex = 			
			new RangeBasedDistributedIndex<>(
				servers, 
				this, 
				List.of(WAREHOUSE_ID_COLUMN, ID_COLUMN),
				indexRuleMap
			);
		
		this.indices = List.of(idIndex);
	}

	@Override
	public Item newInstance(TransactionId txId, RecordId recordId) {
		return new Item(txId, recordId);
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
	protected Collection<DistributedIndex<Item>> indices() {
		return indices;
	}

	@Override
	protected DistributedIndex<Item> getMaintainingComputerDecidingIndex() {
		return idIndex;
	}
}
