package com.dianemodb.tpcc.init;

import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Item;

public class ItemInitializer extends TpccDataInitializer {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ItemInitializer.class.getName());

	public ItemInitializer(SQLServerApplication application) {
		super(application);
	}

	private static final int ITEM_PER_TX = 1000;

	@Override
	protected ModificationCollection createModificationCollection(
			TransactionId txId, 
			int batchNumber
	) {
		ModificationCollection modificationCollection = new ModificationCollection();
		
		for( int i = 0 ; i < ITEM_PER_TX; i++ ) {
			Item item = new Item(txId, null);
			
			int itemId = (batchNumber * ITEM_PER_TX)  + i;
			item.setItemId(itemId);
			
			item.setIm(randomInt(1, 10000));
			item.setName(randomString(14, 24));
			item.setPrice(randomFloat(1, 100));
			
			String data = randomString(26, 50);
			
			if( itemId % 10 == 0 ) {
				int brandMarkerPosition = randomInt(0, data.length() - 8);
				
				data = 
					data.substring(0, brandMarkerPosition) 
					+ Constants.BRAND_SIGNAL_KEY 
					+ data.substring(brandMarkerPosition + 8);
			}
			
			item.setData(data);
			
			modificationCollection.addInsert(item, application);
		}
		
		LOGGER.info("Item batch {}, number {}", batchNumber + 1, (batchNumber + 1) * ITEM_PER_TX );
		
		return modificationCollection;
	}

	@Override
	public int numberOfBatches() {
		// safeguard for modifications
		assert Constants.ITEM_NUMBER % ITEM_PER_TX == 0;
		return Constants.ITEM_NUMBER / ITEM_PER_TX;
	}
}
