package com.dianemodb.tpcc.init;

import java.math.BigDecimal;

import com.dianemodb.ModificationCollection;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Warehouse;

public class WarehouseInitializer extends TpccDataInitializer {

	public WarehouseInitializer(SQLServerApplication application) {
		super(application);
	}

	@Override
	public int numberOfBatches() {
		return 1;
	}

	@Override
	protected ModificationCollection createModificationCollection(TransactionId txId, int batchNumber) {
		ModificationCollection modificationCollection = new ModificationCollection();
		for(short i=0 ; i < Constants.NUMBER_OF_WAREHOUSES; i++) {
			Warehouse warehouse = new Warehouse(txId, null);
			randomValues(warehouse);
			
			warehouse.setYtd(new BigDecimal(3000000));
			warehouse.setPublicId(i);

			modificationCollection.addInsert(warehouse, application);
		}

		return modificationCollection;
	}

}
