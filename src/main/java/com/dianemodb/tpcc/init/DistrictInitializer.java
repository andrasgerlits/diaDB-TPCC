package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.District;

public class DistrictInitializer extends TpccDataInitializer {
	
	public DistrictInitializer(DianemoApplication application) {
		super(application);
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		
		List<UserRecord> records = new LinkedList<>();
		for(short warehouseId = 0; warehouseId < Constants.NUMBER_OF_WAREHOUSES; warehouseId++) {
			for( byte i = 0; i < Constants.DISTRICT_PER_WAREHOUSE; i++ ) {
				District district = new District(txId, null);
				randomValues(district);
				
				district.setYtd(new BigDecimal(30000));
				district.setNextOid(3001);
				district.setId(i);
				district.setWarehouseId(warehouseId);
				
				records.add(district);
			}
		}
		
		return records;
	}

	@Override
	public int numberOfBatches() {
		return 1;
	}

}
