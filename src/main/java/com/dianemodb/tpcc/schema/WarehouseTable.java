package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.h2impl.UniqueHashCodeBasedDistributedIndex;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.tpcc.entity.Warehouse;

public class WarehouseTable extends AddressAndTaxUserBaseTable<Warehouse> {
	
	public static final String TABLE_NAME = "warehouse";
	
	public static final String ID_COLUMNNAME = "w_id";
	public static final String NAME_COLUMN_NAME = "w_name";
	public static final String STREET_1_COLUMN_NAME = "w_street_1";
	public static final String STREET_2_COLUMN_NAME = "w_street_2";
	public static final String CITY_COLUMN_NAME = "w_city";
	public static final String STATE_COLUMN_NAME = "w_state";
	public static final String ZIP_COLUMN_NAME = "w_zip";
	public static final String TAX_COLUMN_NAME = "w_tax";
	public static final String YTD_COLUMN_NAME = "w_ytd";

	public static final UserRecordTableId ID = new UserRecordTableId(WAREHOUSE_TABLE_ID);

	private final Collection<DistributedIndex<Warehouse>> indices;

	private final List<RecordColumn<Warehouse, ?>> columns;

	private final RecordColumn<Warehouse, Short> idColumn;
	
	private final UniqueHashCodeBasedDistributedIndex<Warehouse> index;
	
	protected WarehouseTable(List<ServerComputerId> servers) {
		super(
			TABLE_NAME,
			ID,
			NAME_COLUMN_NAME, 
			STREET_1_COLUMN_NAME, 
			STREET_2_COLUMN_NAME, 
			CITY_COLUMN_NAME, 
			STATE_COLUMN_NAME, 
			ZIP_COLUMN_NAME, 
			TAX_COLUMN_NAME, 
			YTD_COLUMN_NAME
		);
		
		this.idColumn = 
				new RecordColumn<>(
						new ShortColumn(ID_COLUMNNAME), 
						Warehouse::getPublicId,
						Warehouse::setPublicId
				);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.add(idColumn);

		this.index = 
				new UniqueHashCodeBasedDistributedIndex<>(
						servers, 
						this, 
						List.of(idColumn)
				);
		
		this.indices = List.of(index);
	}

	@Override
	public Warehouse newInstance(TransactionId txId, RecordId recordId) {
		return new Warehouse(txId, recordId);
	}

	@Override
	public Class<Warehouse> entityClass() {
		return Warehouse.class;
	}

	@Override
	protected Collection<DistributedIndex<Warehouse>> indices() {
		return indices;
	}

	public RecordColumn<Warehouse, Short> getPublicIdColumn() {
		return idColumn;
	}

	@Override
	protected DistributedIndex<Warehouse> getMaintainingComputerDecidingIndex() {
		return index;
	}
}
