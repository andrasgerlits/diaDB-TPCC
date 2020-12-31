package com.dianemodb.tpcc.query.orderstatus;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.LimitClause;
import com.dianemodb.metaschema.distributed.OrderByClause;
import com.dianemodb.metaschema.distributed.OrderByClause.OrderType;
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

	private static final LimitClause LIMIT_CLAUSE = new LimitClause(1);
	
	private static final OrderByClause<Orders> ORDER_BY_CLAUSE = 
			new OrderByClause<>(List.of(Pair.of(OrdersTable.ORDER_ID_COLUMN, OrderType.DESC)));
	
	public static final String ID = "findMaxOrderIdForCustomer";

	private static final Condition<Orders> CONDITION = 			
			Condition.andEqualsEach(
				List.of(
					OrdersTable.WAREHOUSE_ID_COLUMN,
					OrdersTable.DISTRICT_ID_COLUMN,
					OrdersTable.CUSTOMER_ID_COLUMN
				)
			);

	public FindMaxOrderIdForCustomer(OrdersTable table) {
		super(
			ID,  
			table, 
			table.getCompositeCustomerIndex(),
			CONDITION,
			ORDER_BY_CLAUSE,
			LIMIT_CLAUSE
		);
	}
}
