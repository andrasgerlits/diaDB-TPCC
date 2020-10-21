package com.dianemodb.tpcc.query.delivery;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.schema.OrdersTable;

public class FindOrderByDistrictWarehouseOrderId extends SingleIndexQueryDistributionPlan<Orders>{

	public static final String ID = "FindOrderByOrderId";
	
	public static final String QUERY = 
			"SELECT ";
	
	public FindOrderByDistrictWarehouseOrderId(OrdersTable table ) {
		super(ID, QUERY, table, table.getCompositeIndex());
	}

}
