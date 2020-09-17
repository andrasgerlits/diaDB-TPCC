package com.dianemodb.tpcc.transaction.neworder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.metaschema.RecordAndTransaction;
import com.dianemodb.sql.SingleIndexedColumnQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class FindWarehouseDetailsById extends SingleIndexedColumnQueryDistributionPlan<Warehouse, Short> {

	private static final String QUERY = 
			"SELECT * FROM " + WarehouseTable.TABLE_NAME 
			+ " WHERE " + WarehouseTable.PUBLIC_ID_COLUMNNAME + "=?";


	public FindWarehouseDetailsById(WarehouseTable table) {
		super("findWarehouseDetails", QUERY, table, table.getPublicIdColumn());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Short> toIndexParams(List<?> queryParams) {
		return (List<Short>) queryParams;
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.UNIQUE;
	}

	@Override
	public Map<RecordAndTransaction, Warehouse> findRecords(
			Connection connection, 
			List<? extends Short> parameters,
			List<RecordAndTransaction> recordsToFind
	) throws SQLException 
	{
		return executeSingleParamQuery(
				connection, 
				(Short) FunctionalUtil.singleResult(parameters), 
				QUERY, 
				this::readFromResultSet, 
				(i, stmt) -> stmt.setShort(1, i)
		);
	}

	@Override
	protected Warehouse readFromResultSet(ResultSet rs) {
		return FunctionalUtil.doOrPropagate(() -> ((WarehouseTable) table).readFromResultSet(rs));
	}
}
