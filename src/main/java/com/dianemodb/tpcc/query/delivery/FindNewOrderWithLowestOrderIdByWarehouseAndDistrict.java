package com.dianemodb.tpcc.query.delivery;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.sql.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByWarehouseAndDistrict extends SingleIndexSingleParameterSetQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";

	// select the lowest order-id for the district of the warehouse
	private static final String MIN_ORDER_ID_QUERY = 
			"SELECT oo.* "
			+ "FROM ("
				+ "SELECT MIN(" + NewOrdersTable.ORDER_ID_COLUMN_NAME + ") mo, "
						+ NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + " mw, "
						+ NewOrdersTable.DISTRICT_ID_COLUMN_NAME + " md "
						
				+ " FROM " + NewOrdersTable.TABLE_NAME
				
				+ " WHERE " + NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + " =? "
					+ " AND "+ NewOrdersTable.DISTRICT_ID_COLUMN_NAME + " =? " 
					
				+ " GROUP BY " 
						+ NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + ", "
						+ NewOrdersTable.DISTRICT_ID_COLUMN_NAME 
			+ ") orr "
			+ "JOIN "+ NewOrdersTable.TABLE_NAME + " oo "
				+ "ON oo." + NewOrdersTable.WAREHOUSE_ID_COLUMN_NAME + "=orr.mw "
					+ "AND oo." + NewOrdersTable.DISTRICT_ID_COLUMN_NAME + "=orr.md "
					+ "AND oo." + NewOrdersTable.ORDER_ID_COLUMN_NAME + "=orr.mo"; 
	
	public FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(NewOrdersTable table) {
		super(
			ID, 
			MIN_ORDER_ID_QUERY, 
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
