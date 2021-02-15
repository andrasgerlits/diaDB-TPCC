package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.H2SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class FindWarehouseDetailsById extends H2SingleParameterSetQueryDistributionPlan<Warehouse> {

	public static final String ID = "findWarehouseDetails";

	public FindWarehouseDetailsById(WarehouseTable table) {
		super(ID, table, table.getIdIndex());
	}

}
