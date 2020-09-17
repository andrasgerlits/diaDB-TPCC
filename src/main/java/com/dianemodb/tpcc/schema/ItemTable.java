package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.tpcc.entity.Item;

public class ItemTable extends TpccBaseTable<Item> {

	public static final UserRecordTableId ID = new UserRecordTableId(ITEM_TABLE_ID);
	
	private static final String TABLE_NAME = "item";
	
	public static final String ID_COLUMN_NAME = "i_id";
	public static final String IM_ID_COLUMN_NAME = "i_im_id";
	public static final String NAME_COLUMN_NAME = "i_name";
	public static final String PRICE_COLUMN_NAME = "i_price";
	public static final String DATA_COLUMN_NAME = "i_data";
	
	public static final RecordColumn<Item, TransactionId> TX_ID_COLUMN = TX_ID();
	public static final RecordColumn<Item, RecordId> RECORD_ID_COLUMN = RECORD_ID();

	private static final List<RecordColumn<Item, ?>> COLUMNS = 
			List.of(
				new RecordColumn<>(new IntColumn(ID_COLUMN_NAME), Item::getPublicId),
				new RecordColumn<>(new IntColumn(IM_ID_COLUMN_NAME), Item::getIm),
				new RecordColumn<>(new StringColumn(NAME_COLUMN_NAME), Item::getName),
				new RecordColumn<>(new BigDecimalColumn(PRICE_COLUMN_NAME, 5, 2), Item::getPrice),
				new RecordColumn<>(new StringColumn(DATA_COLUMN_NAME), Item::getData)
		);

	private final List<RecordColumn<Item, ?>> columns;

	private final Collection<DistributedIndex<Item, ?>> indices;
	
	public ItemTable(Collection<ServerComputerId> servers) {
		super(ID, TABLE_NAME);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.addAll(COLUMNS);
		
		this.indices = List.of();
	}

	@Override
	public Item newInstance(TransactionId txId, RecordId recordId) {
		return new Item(txId, recordId);
	}

	@Override
	public Class<Item> entityClass() {
		return Item.class;
	}

	@Override
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			Item thing
	) {
		return null;
	}

	@Override
	protected List<RecordColumn<Item, ?>> columns() {
		return columns;
	}

	@Override
	public RecordColumn<Item, TransactionId> getTransactionIdColumn() {
		return TX_ID_COLUMN;
	}

	@Override
	protected Collection<DistributedIndex<Item, ?>> indices() {
		return indices;
	}
}
