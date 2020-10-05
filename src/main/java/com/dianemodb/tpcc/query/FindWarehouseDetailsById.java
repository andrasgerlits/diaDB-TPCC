package com.dianemodb.tpcc.query;

import java.util.List;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class FindWarehouseDetailsById extends SingleIndexQueryDistributionPlan<Warehouse> {

	public static final String ID = "findWarehouseDetails";
	
	private static final String QUERY = 
			"SELECT * FROM " + WarehouseTable.TABLE_NAME 
			+ " WHERE " + WarehouseTable.PUBLIC_ID_COLUMNNAME + "=?";


	public FindWarehouseDetailsById(WarehouseTable table) {
		super(ID, QUERY, table, List.of(table.getPublicIdColumn()));
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}
}
