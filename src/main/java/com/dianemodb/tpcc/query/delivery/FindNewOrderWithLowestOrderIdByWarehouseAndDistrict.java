package com.dianemodb.tpcc.query.delivery;

import java.util.List;

import com.dianemodb.h2impl.SingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.LimitClause;
import com.dianemodb.metaschema.distributed.OrderByClause;
import com.dianemodb.metaschema.distributed.OrderByClause.OrderType;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByWarehouseAndDistrict extends SingleParameterSetQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";
	
	/*
	 * SELECT t.* 
	 * 	FROM new_orders t 
	 * 	WHERE t.w_id=? AND t.d_id=? 
	 *  ORDER BY t.o_id DESC 
	 *  LIMIT 1
	 */
	public FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(NewOrdersTable table) {
		super(
			ID, 
			table, 
			table.getCompositeIndex(),
			Condition.andEqualsEach(
				List.of(
					NewOrdersTable.WAREHOUSE_ID_COLUMN, 
					NewOrdersTable.DISTRICT_ID_COLUMN
				)
			),
			new OrderByClause<>(List.of(NewOrdersTable.ORDER_ID_COLUMN), OrderType.DESC),
			new LimitClause(1)
		);
	}
}
