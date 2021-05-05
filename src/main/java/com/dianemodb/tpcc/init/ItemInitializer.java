package com.dianemodb.tpcc.init;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.UserRecord;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Item;
import com.dianemodb.tpcc.schema.ItemTable;

public class ItemInitializer extends TpccDataInitializer {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ItemInitializer.class.getName());

	public ItemInitializer(DianemoApplication application) {
		super(application);
	}

	private static final int ITEM_PER_TX = 10;

	@Override
	protected List<UserRecord> createModificationCollection(
			TransactionId txId, 
			int batchNumber
	) {
		List<UserRecord> records = new LinkedList<>();
		ItemTable table = (ItemTable) application.getTableById(ItemTable.ID);
		
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
			
			Iterator<ServerComputerId> serverIter = table.getRecordMaintainingComputers().iterator();
			
			// create a copy of each item for each server, it's read-only, so safe to do
			for(short j = 0; serverIter.hasNext(); j++) {
				serverIter.next();
				
				Item clone = prototype.shallowClone(application, txId);
				clone.setDistId(j);
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
