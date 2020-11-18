package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.ModificationCollection;
import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.District;

public class DistrictInitializer extends TpccDataInitializer {
	
	public DistrictInitializer(SQLServerApplication application) {
		super(application);
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		short warehouseId = (short) batchNumber;
		
		List<UserRecord> records = new LinkedList<>();
		for( byte i = 0; i < Constants.DISTRICT_PER_WAREHOUSE; i++ ) {
			District district = new District(txId, null);
			randomValues(district);
			
			district.setYtd(new BigDecimal(30000));
			district.setNextOid(3001);
			district.setId(i);
			district.setWarehouseId(warehouseId);
			
			records.add(district);
		}
		
		return records;
	}

	@Override
	public int numberOfBatches() {
		return Constants.NUMBER_OF_WAREHOUSES;
	}

}
