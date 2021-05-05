package com.dianemodb.tpcc.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.h2impl.H2RangeBasedDistributedIndex;
import com.dianemodb.id.IndexTableId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.column.ByteColumn;
import com.dianemodb.metaschema.column.IntColumn;
import com.dianemodb.metaschema.column.RecordColumn;
import com.dianemodb.metaschema.column.ShortColumn;
import com.dianemodb.metaschema.distributed.ServerComputerIdNarrowingRule;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.query.IndexColumnDefinition;
import com.dianemodb.query.NullRule;
import com.dianemodb.query.RangeBasedDistributedIndex;
import com.dianemodb.tpcc.entity.District;


public class DistrictTable extends AddressAndTaxUserBaseTable<District> {
	
	public static final String TABLE_NAME = "district";

	public static final String ID_COLUMNNAME = "d_id";
	public static final String NAME_COLUMN_NAME = "d_name";
	public static final String STREET_1_COLUMN_NAME = "d_street_1";
	public static final String STREET_2_COLUMN_NAME = "d_street_2";
	public static final String CITY_COLUMN_NAME = "d_city";
	public static final String STATE_COLUMN_NAME = "d_state";
	public static final String ZIP_COLUMN_NAME = "d_zip";
	public static final String TAX_COLUMN_NAME = "d_tax";
	public static final String YTD_COLUMN_NAME = "d_ytd";

	public static final String WAREHOUSE_ID_COLUMNNAME = "d_w_id";
	public static final String NEXT_OID_COLUMN_NAME = "d_next_o_id";

	private static final RecordColumn<District, Short> WAREHOUSE_COLUMN = 
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMNNAME), 
					District::getWarehouseId, 
					District::setWarehouseId
			);

	private static final RecordColumn<District, Integer> NEXT_OID_COLUMN = 
			new RecordColumn<>(
					new IntColumn(NEXT_OID_COLUMN_NAME), 
					District::getNextOid, 
					District::setNextOid
			);

	public static final RecordColumn<District, Byte> ID_COLUMN = 
			new RecordColumn<>(
					new ByteColumn(ID_COLUMNNAME), 
					District::getId, 
					District::setId
			);
	
	public static final UserRecordTableId ID = new UserRecordTableId(DISTRICT_TABLE_ID);
	
	public static <R extends UserRecord> Map<RecordColumn<R, ?>, ServerComputerIdNarrowingRule> getDistrictBasedRoundRobinRules(
			RecordColumn<R, Short> warehouseColumn, 
			RecordColumn<R, Byte> districtIdColumn
	) {
		return new HashMap<>(
				Map.of(
					// round-robin per warehouse
					warehouseColumn, WarehouseTable.getWarehouseDistributionRule(),
					districtIdColumn, NullRule.INSTANCE
				)
			);
	}

	

	private final List<RecordColumn<District, ?>> columns;
	private final Collection<UserRecordIndex<District>> indices;
	private final RangeBasedDistributedIndex<District> compositeIndex;
	
	public DistrictTable(Topology servers) {
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
		
		this.columns = super.columns();
		columns.add(ID_COLUMN);
		columns.add(WAREHOUSE_COLUMN);
		columns.add(NEXT_OID_COLUMN);
		
		compositeIndex = 
				new H2RangeBasedDistributedIndex<>(
						new IndexTableId(0, ID),
						servers,
						this, 
						List.of(
							warehouseIndexColumnDefinition, 
							new IndexColumnDefinition<>(ID_COLUMN)
						)
				);
		
		this.indices = List.of(compositeIndex);
	}

	@Override
	public List<RecordColumn<District, ?>> columns() {
		return columns;
	}

	@Override
	public District newInstance(TransactionId txId, RecordId recordId) {
		return new District(txId, recordId);
	}

	@Override
	public Class<District> entityClass() {
		return District.class;
	}

	@Override
	protected Collection<UserRecordIndex<District>> indices() {
		return indices;
	}

	public UserRecordIndex<District> getCompositeIndex() {
		return compositeIndex;
	}

	@Override
	public RecordColumn<District, Short> getWarehouseIdColumn() {
		return WAREHOUSE_COLUMN;
	}
}
