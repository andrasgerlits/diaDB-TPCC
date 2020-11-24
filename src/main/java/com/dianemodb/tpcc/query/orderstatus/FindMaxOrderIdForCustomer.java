package com.dianemodb.tpcc.query.orderstatus;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.sql.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.schema.OrdersTable;

/**
 * <p>
 * This query presumes that for a specific customer, 
 * all of their orders live on a particular computer instance, since the order-id isn't
 * part of the query.
 * 
 * <p>
 * This could be changed by introducing the order-id as part of an index on OrdersTable
 * and specifying a realistic range.
 * */
public class FindMaxOrderIdForCustomer extends SingleIndexSingleParameterSetQueryDistributionPlan<Orders> {

	public static final String ID = "findMaxOrderIdForCustomer";
	
	private static final String QUERY = 
			"SELECT oo.* "
			+ "FROM ("
				+ "SELECT MAX(" + OrdersTable.ORDER_ID_COLUMN_NAME + ") mo, "
						+ OrdersTable.WAREHOUSE_ID_COLUMN_NAME + " mw, "
						+ OrdersTable.DISTRICT_ID_COLUMN_NAME + " md, "
						+ OrdersTable.CUSTOMER_ID_COLUMN_NAME + " mc "
						
				+ " FROM " + OrdersTable.TABLE_NAME
				
				+ " WHERE " + OrdersTable.WAREHOUSE_ID_COLUMN_NAME + " =? "
					+ " AND "+ OrdersTable.DISTRICT_ID_COLUMN_NAME + " =? "
					+ " AND "+ OrdersTable.CUSTOMER_ID_COLUMN_NAME + " =? "
					
				+ " GROUP BY " 
						+ OrdersTable.WAREHOUSE_ID_COLUMN_NAME + ", "
						+ OrdersTable.DISTRICT_ID_COLUMN_NAME + "," 
						+ OrdersTable.CUSTOMER_ID_COLUMN_NAME
			+ ") orr "
			+ "JOIN "+ OrdersTable.TABLE_NAME + " oo "
				+ "ON oo." + OrdersTable.WAREHOUSE_ID_COLUMN_NAME + "=orr.mw "
					+ "AND oo." + OrdersTable.DISTRICT_ID_COLUMN_NAME + "=orr.md "
					+ "AND oo." + OrdersTable.CUSTOMER_ID_COLUMN_NAME + "=orr.mc "
					+ "AND oo." + OrdersTable.ORDER_ID_COLUMN_NAME + "=orr.mo"; 

	public FindMaxOrderIdForCustomer(OrdersTable table) {
		super(
			ID, 
			QUERY, 
			table, 
			table.getCompositeCustomerIndex()
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
