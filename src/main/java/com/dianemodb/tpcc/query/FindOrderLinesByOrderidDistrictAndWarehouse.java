package com.dianemodb.tpcc.query;

import java.util.List;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class FindOrderLinesByOrderidDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<OrderLine> {

	public FindOrderLinesByOrderidDistrictAndWarehouse(OrderLineTable table) {
		super(
			"findOrderLineByIdDistrictAndWarehouse", 
			"SELECT * FROM " + OrderLineTable.TABLE_NAME 
			+ " WHERE " + OrderLineTable.ORDER_ID_COLUMN_NAME + "=?"
					+ " AND " + OrderLineTable.DISTRICT_ID_COLUMN_NAME + "=?"
					+ " AND " + OrderLineTable.WAREHOUSE_ID_COLUMN_NAME + "=?", 
			table, 
			List.of(
				OrderLineTable.ORDER_ID_COLUMN,
				OrderLineTable.DISTRICT_ID_COLUMN,
				OrderLineTable.WAREHOUSE_ID_COLUMN
			)
		);
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}
}
