package com.dianemodb.tpcc.init;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.UserRecord;
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
	
	private static final int TOTAL_NUMBER_OF_BATCHES = 
			Constants.STOCK_PER_WAREHOUSE * Constants.NUMBER_OF_WAREHOUSES / ITEM_PER_TX;

	@Override
	public int numberOfBatches() {
		return TOTAL_NUMBER_OF_BATCHES;
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		List<UserRecord> records = new LinkedList<>();
		
		for( int i = 0 ; i < ITEM_PER_TX; i++ ) {
			int totalOffset = batchNumber + (TOTAL_NUMBER_OF_BATCHES * i);
			
			int[][] numbers =  calculatePositions(totalOffset, Constants.STOCK_PER_WAREHOUSE);
			short warehouseId = (short) numbers[0][0];
			int itemId = numbers[0][1];
			
			Stock stock = new Stock(txId, null);
			stock.setWarehouseId( warehouseId );
			
			stock.setQuantity((short) randomInt(10, 100));
			stock.setItemId(itemId);
			
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
			
			records.add(stock);
		}
		
		//LOGGER.info("Stock batch {} / {}", batchNumber, numberOfBatches() );
		return records;
	}

}
