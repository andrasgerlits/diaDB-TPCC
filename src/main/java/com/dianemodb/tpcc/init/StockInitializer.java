package com.dianemodb.tpcc.init;

import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Stock;

public class StockInitializer extends TpccDataInitializer {
	
	public StockInitializer(SQLServerApplication application) {
		super(application);
	}

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(StockInitializer.class.getName());
	private static final int ITEM_PER_TX = 1000;

	@Override
	public int numberOfBatches() {
		return Constants.STOCK_PER_WAREHOUSE * Constants.NUMBER_OF_WAREHOUSES / ITEM_PER_TX;
	}

	@Override
	protected ModificationCollection createModificationCollection(TransactionId txId, int batchNumber) {
		ModificationCollection modificationCollection = new ModificationCollection();
		
		for( int i = 0 ; i < ITEM_PER_TX; i++ ) {
			int totalOffset = batchNumber * ITEM_PER_TX + i;
			
			Stock stock = new Stock(txId, null);
			stock.setWarehouseId( (short) (totalOffset % Constants.NUMBER_OF_WAREHOUSES));
			
			stock.setQuantity((short) randomInt(10, 100));
			stock.setItemId(randomInt(0, Constants.ITEM_NUMBER));
			
			stock.setDist1(randomString(24,24));
			stock.setDist2(randomString(24,24));
			stock.setDist3(randomString(24,24));
			stock.setDist4(randomString(24,24));
			stock.setDist5(randomString(24,24));
			stock.setDist6(randomString(24,24));
			stock.setDist7(randomString(24,24));
			stock.setDist8(randomString(24,24));
			stock.setDist9(randomString(24,24));
			stock.setDist10(randomString(24,24));
			
			stock.setYtd(0L);
			stock.setRemoteCnt((short) 0);
			stock.setOrderCnt((short) 0);
			
			// every 10th item is "original"
			String data;
			if(randomInt(0, 10) == 1) {
				data = Constants.BRAND_SIGNAL_KEY;
			}
			else {
				data = randomString(26,50);
			}
			
			stock.setData(data);
			
			modificationCollection.addInsert(stock, application);
		}
		
		//LOGGER.info("Stock batch {} / {}", batchNumber, numberOfBatches() );
		return modificationCollection;
	}

}
