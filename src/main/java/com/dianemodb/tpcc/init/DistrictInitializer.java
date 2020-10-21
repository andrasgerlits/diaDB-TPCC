package com.dianemodb.tpcc.init;

import java.math.BigDecimal;

import com.dianemodb.ModificationCollection;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.District;

public class DistrictInitializer extends TpccDataInitializer {
	
	public DistrictInitializer(SQLServerApplication application) {
		super(application);
	}

	@Override
	protected ModificationCollection createModificationCollection(TransactionId txId, int batchNumber) {
		short warehouseId = (short) batchNumber;
		
		ModificationCollection modificationCollection = new ModificationCollection();
		for( byte i = 0; i < Constants.DISTRICT_PER_WAREHOUSE; i++ ) {
			District district = new District(txId, null);
			randomValues(district);
			
			district.setYtd(new BigDecimal(30000));
			district.setNextOid(3001);
			district.setId(i);
			district.setWarehouseId(warehouseId);
			
			modificationCollection.addInsert(district, application);
		}
		
		return modificationCollection;
	}

	@Override
	public int numberOfBatches() {
		return Constants.NUMBER_OF_WAREHOUSES;
	}

}
