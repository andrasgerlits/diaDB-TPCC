package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.QueryDefinition;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.Operator;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class StockLevel extends TpccTestProcess {
	
	private final byte districtId;
	private final int stockThreshold;

	protected StockLevel(
			Random random, 
			ServerComputerId txComputer,
			DianemoApplication application, 
			short warehouseId,
			byte districtId,
			String uuid
	) {
		// no keying-time, no think-time and no variance
		super(random, application, txComputer, 0, 0, warehouseId, uuid);
		this.districtId = districtId;
		this.stockThreshold = TpccDataInitializer.randomInt(10,20);
	}
	
	public boolean isTerminalBased() {
		return false;
	}

	@Override
	protected Result startTx() {
		Envelope districtQuery = 
				query(
					NewOrder.FIND_DISTRICT_BY_WH_DIST_ID,
					List.of(terminalWarehouseId, districtId)
				);
		
		return of(List.of(districtQuery), this::findOrderLines);
	}
	
	@SuppressWarnings("unchecked")
	private Result findOrderLines(List<? extends Object> results) {
		RecordWithVersion<District> district = 
				FunctionalUtil.singleResult( (List<RecordWithVersion<District>>) results.iterator().next() );
		
		int nextOid = district.getRecord().getNextOid();
		
		Envelope queryStock = 
				query(
					new QueryDefinition<>(
						OrderLineTable.ID, 			
						new Condition<OrderLine>(
							List.of(
								Pair.of(OrderLineTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
								Pair.of(OrderLineTable.DISTRICT_ID_COLUMN, Operator.EQ),
								
								// order-id <= ? AND order-id >= ?
								Pair.of(OrderLineTable.ORDER_ID_COLUMN, Operator.LTE),
								Pair.of(OrderLineTable.ORDER_ID_COLUMN, Operator.GTE)
							)
						), 
						false
					),
					List.of(terminalWarehouseId, districtId, nextOid, nextOid - 20)
				);
		
		return of(List.of(queryStock), this::commit);
	}
}
