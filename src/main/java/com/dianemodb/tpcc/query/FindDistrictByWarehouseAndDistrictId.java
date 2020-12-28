package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.schema.DistrictTable;

public class FindDistrictByWarehouseAndDistrictId extends SingleParameterSetQueryDistributionPlan<District> {
	
	public static final String ID = "findDistrictById";

	public FindDistrictByWarehouseAndDistrictId(DistrictTable table) {
		// warehouse, district
		super(ID, table, table.getCompositeIndex());
	}

}
