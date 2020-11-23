package com.dianemodb.tpcc.query;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class FindWarehouseDetailsById extends SingleIndexQueryDistributionPlan<Warehouse> {

	public static final String ID = "findWarehouseDetails";

	public FindWarehouseDetailsById(WarehouseTable table) {
		super(ID, table, table.getIdIndex());
	}

}
