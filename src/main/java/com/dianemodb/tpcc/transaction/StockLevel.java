package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Random;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.FindDistrictByIdAndWarehouse;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderIdRangeDistrictAndWarehouse;

public class StockLevel extends TpccTestProcess {
	
	private final short districtId;
	private final int stockThreshold;

	protected StockLevel(
			Random random, 
			ServerComputerId txComputer,
			SQLServerApplication application, 
			short warehouseId,
			short districtId
	) {
		super(random, application, txComputer, 5000, 2000, 5000, warehouseId);
		this.districtId = districtId;
		this.stockThreshold = TpccDataInitializer.randomInt(10,20);
	}

	@Override
	protected Result startTx() {
		Envelope districtQuery = 
				query(
					FindDistrictByIdAndWarehouse.ID,
					List.of(terminalWarehouseId, districtId)
				);
		
		return of(List.of(districtQuery), this::findOrderLines);
	}
	
	private Result findOrderLines(List<? extends Object> results) {
		RecordWithVersion<District> district = 
				FunctionalUtil.singleResult( (List<RecordWithVersion<District>>) results.iterator().next() );
		
		int nextOid = district.getRecord().getNextOid();
		Envelope queryStock = 
				query(
					FindOrderLinesByOrderIdRangeDistrictAndWarehouse.ID,
					List.of(terminalWarehouseId, districtId, nextOid, nextOid - 20)
				);
		
		return of(List.of(queryStock), this::commit);
	}

}
