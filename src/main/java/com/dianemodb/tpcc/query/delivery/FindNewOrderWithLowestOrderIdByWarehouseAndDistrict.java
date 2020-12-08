package com.dianemodb.tpcc.query.delivery;

import com.dianemodb.h2impl.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.AggregateFunction;
import com.dianemodb.metaschema.distributed.AggregateType;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByWarehouseAndDistrict extends SingleIndexSingleParameterSetQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";
	
	public FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(NewOrdersTable table) {
		super(
			ID, 
			table, 
			table.getCompositeIndex(),
			new AggregateFunction<>(NewOrdersTable.ORDER_ID_COLUMN, AggregateType.MIN) 
		);
	}
}
