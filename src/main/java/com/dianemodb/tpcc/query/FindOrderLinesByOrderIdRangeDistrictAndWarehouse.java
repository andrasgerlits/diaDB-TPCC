package com.dianemodb.tpcc.query;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class FindOrderLinesByOrderIdRangeDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<OrderLine> {

	public static final String ID = "findOrderLinesByOrderIdRange";
	
	private static final String QUERY = 
			"SELECT * FROM " + OrderLineTable.TABLE_NAME 
				+ " WHERE " + OrderLineTable.WAREHOUSE_ID_COLUMN_NAME + " =? "
					+ " AND " + OrderLineTable.DISTRICT_ID_COLUMN_NAME + " =? "
					+ " AND " + OrderLineTable.ORDER_ID_COLUMN_NAME + " >= ? "
					+ "AND " + OrderLineTable.ORDER_ID_COLUMN_NAME + " <= ?";
	
	public FindOrderLinesByOrderIdRangeDistrictAndWarehouse(OrderLineTable table) {
		super(ID, QUERY, table, table.getOrderIdRangeIndex().getUserRecordColumns());
	}
}
