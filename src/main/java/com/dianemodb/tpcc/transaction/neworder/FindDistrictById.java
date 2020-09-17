package com.dianemodb.tpcc.transaction.neworder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.metaschema.RecordAndTransaction;
import com.dianemodb.sql.SingleIndexedColumnQueryDistributionPlan;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.schema.DistrictTable;

public class FindDistrictById extends SingleIndexedColumnQueryDistributionPlan<District, Byte> {
	
	private static final String QUERY = 
			"SELECT * "
			+ "FROM " + DistrictTable.TABLE_NAME 
			+ " WHERE " + DistrictTable.PUBLIC_ID_COLUMNNAME + "=?";

	public FindDistrictById(DistrictTable table) {
		super("findDistrictById", QUERY, table, DistrictTable.PUBLIC_ID_COLUMN);
	}

	@Override
	public List<Byte> toIndexParams(List<?> queryParams) {
		return (List<Byte>) queryParams;
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.UNIQUE;
	}

	@Override
	public Map<RecordAndTransaction, District> findRecords(
			Connection connection, 
			List<? extends Byte> parameters,
			List<RecordAndTransaction> recordsToFind
	) throws SQLException {
		return executeSingleParamQuery(
				connection, 
				(Byte) FunctionalUtil.singleResult(parameters), 
				QUERY, 
				this::readFromResultSet, 
				(i, stmt) -> stmt.setShort(1, i)
		);
	}

	@Override
	protected District readFromResultSet(ResultSet rs) {
		return FunctionalUtil.doOrPropagate(() -> ((DistrictTable) table).readFromResultSet(rs));
	}
}
