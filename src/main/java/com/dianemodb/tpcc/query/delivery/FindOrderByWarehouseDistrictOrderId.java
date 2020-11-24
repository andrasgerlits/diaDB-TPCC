package com.dianemodb.tpcc.query.delivery;

import com.dianemodb.sql.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.schema.OrdersTable;

public class FindOrderByWarehouseDistrictOrderId extends SingleIndexSingleParameterSetQueryDistributionPlan<Orders>{

	public static final String ID = "FindOrderByOrderId";
	
	public FindOrderByWarehouseDistrictOrderId(OrdersTable table ) {
		super(ID, table, table.getCompositeIndex());
	}
}
