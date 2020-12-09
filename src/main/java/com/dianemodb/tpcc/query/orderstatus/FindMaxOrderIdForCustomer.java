package com.dianemodb.tpcc.query.orderstatus;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.h2impl.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.metaschema.distributed.AggregateFunction;
import com.dianemodb.metaschema.distributed.AggregateType;
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

	public FindMaxOrderIdForCustomer(OrdersTable table) {
		super(
			ID,  
			table, 
			table.getCompositeCustomerIndex(),
			new AggregateFunction<Orders, Integer>(OrdersTable.ORDER_ID_COLUMN, AggregateType.MAX)
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
