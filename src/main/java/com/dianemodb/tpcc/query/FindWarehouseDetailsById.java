package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class FindWarehouseDetailsById extends SingleParameterSetQueryDistributionPlan<Warehouse> {

	public static final String ID = "findWarehouseDetails";

	public FindWarehouseDetailsById(WarehouseTable table) {
		super(ID, table, table.getIdIndex());
	}

}
