package com.dianemodb.tpcc.query.delivery;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderByDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<NewOrders>{

	public FindNewOrderByDistrictAndWarehouse(NewOrdersTable table) {
		super(
			"findNewOrdersByDistrictAndWarehouse", 
			"SELECT * FROM " + NewOrdersTable.TABLE_NAME 
			+ " WHERE " + NewOrdersTable.DISTRICT_ID_COLUMN_NAME + "=?"
					+ " AND " + NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + "=?", 
			table, 
			NewOrdersTable.DISTRICT_WAREHOUSE_COLUMNS
		);
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}
}
