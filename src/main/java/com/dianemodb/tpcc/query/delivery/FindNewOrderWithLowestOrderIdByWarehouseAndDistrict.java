package com.dianemodb.tpcc.query.delivery;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.LimitClause;
import com.dianemodb.metaschema.distributed.OrderByClause;
import com.dianemodb.metaschema.distributed.OrderByClause.OrderType;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

/*
 * SELECT t.* 
 * 	FROM new_orders t 
 * 	WHERE t.w_id=? AND t.d_id=? 
 *  ORDER BY t.o_id ASC 
 *  LIMIT 1
 */
public class FindNewOrderWithLowestOrderIdByWarehouseAndDistrict extends SingleParameterSetQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";
	
	private static final Condition<NewOrders> CONDITION = 
			Condition.andEqualsEach(
				List.of(
					NewOrdersTable.WAREHOUSE_ID_COLUMN, 
					NewOrdersTable.DISTRICT_ID_COLUMN
				)
			);
	
	private static final OrderByClause<NewOrders> ORDER_BY_CLAUSE = 
			new OrderByClause<>(List.of(Pair.of(NewOrdersTable.ORDER_ID_COLUMN, OrderType.ASC)));
	
	private static final LimitClause LIMIT_CLAUSE = new LimitClause(1);
	
	public FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(NewOrdersTable table) {
		super(
			ID, 
			table, 
			table.getCompositeIndex(),
			CONDITION,
			ORDER_BY_CLAUSE,
			LIMIT_CLAUSE
		);
	}
}
