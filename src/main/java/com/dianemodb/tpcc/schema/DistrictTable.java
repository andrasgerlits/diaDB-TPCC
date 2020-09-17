package com.dianemodb.tpcc.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ServerComputerId;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.ByteColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.entity.Warehouse;


public class DistrictTable extends AddressAndTaxUserBaseTable<District> {
	
	public static final String TABLE_NAME = "district";

	public static final String PUBLIC_ID_COLUMNNAME = "d_id";
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
			new RecordColumn<>(new ShortColumn(WAREHOUSE_ID_COLUMNNAME), District::getWarehouseId);

	private static final RecordColumn<District, Integer> NEXT_OID_COLUMN = 
			new RecordColumn<>(new IntColumn(WAREHOUSE_ID_COLUMNNAME), District::getNextOid);

	public static final RecordColumn<District, Byte> PUBLIC_ID_COLUMN = 
			new RecordColumn<>(new ByteColumn(PUBLIC_ID_COLUMNNAME), District::getPublicId);
	
	public static final UserRecordTableId ID = new UserRecordTableId(DISTRICT_TABLE_ID);

	private final List<RecordColumn<District, ?>> columns;
	private final Collection<DistributedIndex<District, ?>> indices;
	
	protected DistrictTable(Collection<ServerComputerId> servers) {
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
		
		this.columns = super.columns();
		columns.add(PUBLIC_ID_COLUMN);
		columns.add(WAREHOUSE_COLUMN);
		columns.add(NEXT_OID_COLUMN);
		
		this.indices = 
			List.of(
					PUBLIC_ID
			);
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
	public ServerComputerId chooseMaintainingComputer(
			SQLServerApplication application,
			List<ServerComputerId> computers, 
			District thing
	) {
		return null;
	}

	@Override
	protected Collection<DistributedIndex<District, ?>> indices() {
		return indices;
	}

	public District readFromResultSet(ResultSet rs) throws SQLException {
		District district = setFieldsFromResultSet(rs);
		district.setPublicId(rs.getByte(PUBLIC_ID_COLUMNNAME));
		district.setWarehouseId(rs.getShort(WAREHOUSE_ID_COLUMNNAME));
		district.setNextOid(rs.getInt(NEXT_OID_COLUMN_NAME));
		return district;
	}

}