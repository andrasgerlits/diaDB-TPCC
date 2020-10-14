package com.dianemodb.tpcc.query.delivery;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";
	
	public FindNewOrderWithLowestOrderIdByDistrictAndWarehouse(NewOrdersTable table) {
		super(
			ID, 
			"SELECT * FROM " + NewOrdersTable.TABLE_NAME 
			+ " WHERE " + NewOrdersTable.DISTRICT_ID_COLUMN_NAME + "=?"
				+ " AND " + NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + "=?"
				+ " AND " + NewOrdersTable.CARRIER_ID_COLUMN_NAME + "=?", 
			table, 
			table.getCompositeIndex()
		);
	}
	
	@Override
	public List<RecordWithVersion<NewOrders>> aggregateResults(List<RecordWithVersion<NewOrders>> results) {
		// find highest of the reverse comparator (so lowest)
		return QueryStep.findHighest(
				results, 
				(one, other) -> Integer.compare(
									other.getRecord().getOrderId(), 
									one.getRecord().getOrderId()
								)
		);
	}
}
