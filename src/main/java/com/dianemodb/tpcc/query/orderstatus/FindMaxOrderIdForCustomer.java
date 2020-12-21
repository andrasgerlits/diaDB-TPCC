package com.dianemodb.tpcc.query.orderstatus;

import org.h2.expression.aggregate.AggregateType;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.MinMaxFunction;
import com.dianemodb.metaschema.distributed.MinMaxType;
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
public class FindMaxOrderIdForCustomer extends SingleParameterSetQueryDistributionPlan<Orders> {

	public static final String ID = "findMaxOrderIdForCustomer";

	public FindMaxOrderIdForCustomer(OrdersTable table) {
		super(
			ID,  
			table, 
			table.getCompositeCustomerIndex(),
			new MinMaxFunction<Orders, Integer>(OrdersTable.ORDER_ID_COLUMN, MinMaxType.MAX)
		);
	}
}
