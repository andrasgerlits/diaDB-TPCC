package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.Topology;
import com.dianemodb.h2impl.H2RangeBasedDistributedIndex;
import com.dianemodb.id.IndexTableId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.column.RecordColumn;
import com.dianemodb.metaschema.column.ShortColumn;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.query.IntegerRangeBasedIdNarrowingRule;
import com.dianemodb.query.RangeBasedDistributedIndex;
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
	
	public static final RecordColumn<Warehouse, Short> ID_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(ID_COLUMNNAME), 
					Warehouse::getPublicId,
					Warehouse::setPublicId
			);

	public static final UserRecordTableId ID = new UserRecordTableId(WAREHOUSE_TABLE_ID);

	private final Collection<UserRecordIndex<Warehouse>> indices;

	private final List<RecordColumn<Warehouse, ?>> columns;
	
	private final RangeBasedDistributedIndex<Warehouse> index;

	public static IntegerRangeBasedIdNarrowingRule getWarehouseDistributionRule() {
		return new IntegerRangeBasedIdNarrowingRule(1);
	}
	
	public WarehouseTable(Topology servers) {
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
			YTD_COLUMN_NAME,
			servers
		);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.add(ID_COLUMN);

		this.index = 
				new H2RangeBasedDistributedIndex<>(
						new IndexTableId(0, ID),
						servers, 
						this, 
						List.of(super.warehouseIndexColumnDefinition)
				);
		
		this.indices = List.of(index);
	}

	@Override
	public Warehouse newInstance(TransactionId txId, RecordId recordId) {
		return new Warehouse(txId, recordId);
	}
	
	@Override
	public List<RecordColumn<Warehouse, ?>> columns() {
		return columns;
	}

	@Override
	public Class<Warehouse> entityClass() {
		return Warehouse.class;
	}

	@Override
	protected Collection<UserRecordIndex<Warehouse>> indices() {
		return indices;
	}

	public UserRecordIndex<Warehouse> getIdIndex() {
		return index;
	}

	@Override
	public RecordColumn<Warehouse, Short> getWarehouseIdColumn() {
		return ID_COLUMN;
	}
}
