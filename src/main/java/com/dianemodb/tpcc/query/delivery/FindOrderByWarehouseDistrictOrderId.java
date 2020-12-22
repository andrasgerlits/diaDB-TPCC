package com.dianemodb.tpcc.query.delivery;

import com.dianemodb.h2impl.MultipleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.schema.OrdersTable;

public class FindOrderByWarehouseDistrictOrderId extends MultipleParameterSetQueryDistributionPlan<Orders>{

	public static final String ID = "FindOrderByOrderId";
	
	public FindOrderByWarehouseDistrictOrderId(OrdersTable table ) {
		super(ID, table, table.getCompositeIndex());
	}
}
