package com.dianemodb.tpcc.init;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Item;

public class ItemInitializer extends TpccDataInitializer {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ItemInitializer.class.getName());

	public ItemInitializer(SQLServerApplication application) {
		super(application);
	}

	private static final int ITEM_PER_TX = 10;

	@Override
	protected List<UserRecord> createModificationCollection(
			TransactionId txId, 
			int batchNumber
	) {
		List<UserRecord> records = new LinkedList<>();
		
		for( int i = 0 ; i < ITEM_PER_TX; i++ ) {
			Item prototype = new Item(txId, null);
			
			int itemId = (batchNumber * ITEM_PER_TX)  + i;
			prototype.setItemId(itemId);
			
			prototype.setIm(randomInt(1, 10000));
			prototype.setName(randomString(14, 24));
			prototype.setPrice(randomFloat(1, 100));
			
			String data = randomString(26, 50);
			
			if( itemId % 10 == 0 ) {
				int brandMarkerPosition = randomInt(0, data.length() - 8);
				
				data = 
					data.substring(0, brandMarkerPosition) 
					+ Constants.BRAND_SIGNAL_KEY 
					+ data.substring(brandMarkerPosition + 8);
			}
			
			prototype.setData(data);
			
			// create a copy of each item for each warehouse, it's read-only, so safe to do
			for(short j = 0; j < Constants.NUMBER_OF_WAREHOUSES; j++) {
				Item clone = prototype.shallowClone(application, txId);
				clone.setWarehouseId(j);
				records.add(clone);
			}
		}
		
		//LOGGER.info("Item batch {}, number {}", batchNumber + 1, (batchNumber + 1) * ITEM_PER_TX );
		
		return records;
	}

	@Override
	public int numberOfBatches() {
		// safeguard for modifications
		return Constants.ITEM_NUMBER / ITEM_PER_TX;
	}
}
