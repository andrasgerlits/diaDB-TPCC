package com.dianemodb.tpcc.query.delivery;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.h2impl.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.metaschema.QueryStep;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.distributed.AggregateFunction;
import com.dianemodb.metaschema.distributed.AggregateType;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class FindNewOrderWithLowestOrderIdByWarehouseAndDistrict extends SingleIndexSingleParameterSetQueryDistributionPlan<NewOrders>{

	public static final String ID = "findNewOrdersByDistrictAndWarehouse";
	private static final RecordColumn<NewOrders, Integer> AGGREGATE_COLUMN = NewOrdersTable.ORDER_ID_COLUMN;
	
	public FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(NewOrdersTable table) {
		super(
			ID, 
			table, 
			table.getCompositeIndex(),
			new AggregateFunction(AGGREGATE_COLUMN, AggregateType.MIN) 
		);
	}
	
	@Override
	public List<RecordWithVersion<NewOrders>> aggregateResults(List<RecordWithVersion<NewOrders>> results) {
		// find highest of the reverse comparator (so lowest)
		return QueryStep.findHighest(
				results, 
				(one, other) -> Integer.compare(
									AGGREGATE_COLUMN.get(other.getRecord()), 
									AGGREGATE_COLUMN.get(one.getRecord())
								)
		);
	}
}
