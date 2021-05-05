package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Warehouse;

public class WarehouseInitializer extends TpccDataInitializer {

	public WarehouseInitializer(DianemoApplication application) {
		super(application);
	}

	@Override
	public int numberOfBatches() {
		return 1;
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		List<UserRecord> records = new LinkedList<>();
		for(short i=0 ; i < Constants.NUMBER_OF_WAREHOUSES; i++) {
			Warehouse warehouse = new Warehouse(txId, null);
			randomValues(warehouse);
			
			warehouse.setYtd(new BigDecimal(3000000));
			warehouse.setPublicId(i);

			records.add(warehouse);
		}

		return records;
	}

}
