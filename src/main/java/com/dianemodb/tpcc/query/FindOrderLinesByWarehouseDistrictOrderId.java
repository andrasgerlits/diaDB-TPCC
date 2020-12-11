package com.dianemodb.tpcc.query;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class FindOrderLinesByWarehouseDistrictOrderId extends SingleParameterSetQueryDistributionPlan<OrderLine> {

	public static final String ID = "findOrderLineByIdDistrictAndWarehouse";

	public FindOrderLinesByWarehouseDistrictOrderId(OrderLineTable table) {
		super(ID, table, table.getOrderIdRangeIndex());
	}
}
