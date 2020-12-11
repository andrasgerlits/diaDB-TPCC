package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.schema.DistrictTable;

public class FindDistrictByIdAndWarehouse extends SingleParameterSetQueryDistributionPlan<District> {
	
	public static final String ID = "findDistrictById";

	public FindDistrictByIdAndWarehouse(DistrictTable table) {
		// warehouse, district
		super(ID, table, table.getCompositeIndex());
	}

}
