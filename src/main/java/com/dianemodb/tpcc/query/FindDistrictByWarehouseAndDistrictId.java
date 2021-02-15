package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.H2SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.schema.DistrictTable;

public class FindDistrictByWarehouseAndDistrictId extends H2SingleParameterSetQueryDistributionPlan<District> {
	
	public static final String ID = "findDistrictById";

	public FindDistrictByWarehouseAndDistrictId(DistrictTable table) {
		// warehouse, district
		super(ID, table, table.getCompositeIndex());
	}

}
