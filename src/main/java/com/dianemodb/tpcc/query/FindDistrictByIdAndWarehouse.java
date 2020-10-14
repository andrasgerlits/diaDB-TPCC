package com.dianemodb.tpcc.query;

import java.util.List;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.schema.DistrictTable;

public class FindDistrictByIdAndWarehouse extends SingleIndexQueryDistributionPlan<District> {
	
	public static final String ID = "findDistrictById";
	
	private static final String QUERY = 
			"SELECT * "
			+ "FROM " + DistrictTable.TABLE_NAME 
			+ " WHERE " + DistrictTable.ID_COLUMNNAME + "=?";

	public FindDistrictByIdAndWarehouse(DistrictTable table) {
		super(ID, QUERY, table, List.of(DistrictTable.ID_COLUMN));
	}

}
