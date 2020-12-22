package com.dianemodb.tpcc.query;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.Operator;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class FindOrderLinesByOrderIdRangeDistrictAndWarehouse extends SingleParameterSetQueryDistributionPlan<OrderLine> {

	public static final String ID = "findOrderLinesByOrderIdRange";
	
	public FindOrderLinesByOrderIdRangeDistrictAndWarehouse(OrderLineTable table) {
		super(
			ID, 
			table, 
			table.getOrderIdRangeIndex(), 
			new Condition<OrderLine>(
					List.of(
						Pair.of(OrderLineTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
						Pair.of(OrderLineTable.DISTRICT_ID_COLUMN, Operator.EQ),
						
						// order-id <= ? AND order-id >= ?
						Pair.of(OrderLineTable.ORDER_ID_COLUMN, Operator.LTE),
						Pair.of(OrderLineTable.ORDER_ID_COLUMN, Operator.GTE)
					)
			)
		);
	}
}
