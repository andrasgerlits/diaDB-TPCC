package com.dianemodb.tpcc.query.orderstatus;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.schema.OrdersTable;

public class FindMaxOrderIdForCustomer extends SingleIndexQueryDistributionPlan<Orders> {

	public static final String ID = "findMaxOrderIdForCustomer";
	
	private static final String QUERY = 
			"SELECT o.* "
			+ "FROM Orders o "
			+ "WHERE o." + OrdersTable.WAREHOUSE_ID_COLUMN_NAME + "=? "
					+ "AND o." + OrdersTable.DISTRICT_ID_COLUMN_NAME + "=? "
					+ "AND o." + OrdersTable.CUSTOMER_ID_COLUMN_NAME + "=?";  

	public FindMaxOrderIdForCustomer(OrdersTable table) {
		super(
			ID, 
			QUERY, 
			table, 
			table.getCompositeIndex()
		);
	}

	@Override
	public List<RecordWithVersion<Orders>> aggregateResults(List<RecordWithVersion<Orders>> results) {
		return QueryStep.findHighest(
				results, 
				(one, other) -> Integer.compare(
									one.getRecord().getOrderId(), 
									other.getRecord().getOrderId()
								)
		);
	}
}
