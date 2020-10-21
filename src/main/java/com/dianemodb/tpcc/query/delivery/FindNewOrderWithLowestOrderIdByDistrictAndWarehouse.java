package com.dianemodb.tpcc.query.delivery;

import static com.dianemodb.tpcc.schema.NewOrdersTable.*;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";

	// select the lowest order-id for the district of the warehouse
	private static final String MAX_ORDER_ID_QUERY = 
			"SELECT mt.* FROM " + TABLE_NAME + " mt "
			+ " WHERE mt." + DISTRICT_ID_COLUMN_NAME + "=? AND mt." + WAREHOUSE_ID_COLUMN_NAME +"=? "			
			+ " GROUP BY " + DISTRICT_ID_COLUMN_NAME + "," + WAREHOUSE_ID_COLUMN_NAME 
			+ " HAVING mt." + ORDER_ID_COLUMN_NAME + "= MIN(" + ORDER_ID_COLUMN_NAME + ")" ;
	
	public FindNewOrderWithLowestOrderIdByDistrictAndWarehouse(NewOrdersTable table) {
		super(
			ID, 
			MAX_ORDER_ID_QUERY, 
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
