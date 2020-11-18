package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.entity.Orders;

public class OrderInitializer extends PerDistrictDataInitializer {
	
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OrderInitializer.class.getName());
	
	public OrderInitializer(SQLServerApplication application) {
		super(application, Constants.ORDER_PER_DISTRICT);
	}

	private static final int ITEM_PER_TX = 100;

	private static final int TOTAL_NUMBER_OF_BATCHES = 
				Constants.NUMBER_OF_WAREHOUSES 
					* Constants.DISTRICT_PER_WAREHOUSE 
					* Constants.ORDER_PER_DISTRICT 
					/ ITEM_PER_TX;

	@Override
	public int numberOfBatches() {
		return TOTAL_NUMBER_OF_BATCHES;
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		List<UserRecord> records = new LinkedList<>();
		
		short warehouseId = -1;
		byte districtId = -1;
		int orderId = -1;
		
		for( int i = 0 ; i < ITEM_PER_TX; i++ ) {
			int offset = batchNumber + (TOTAL_NUMBER_OF_BATCHES * i);
			warehouseId = getWarehouseId(offset);
			districtId = getDistrictId(offset);
			orderId = getRecordId(offset);
					
			Timestamp now = new Timestamp(System.currentTimeMillis());
			short orderLineCount = (short) randomInt(5, 15);
			short carrierId = randomCarrierId();
			
			int customerId = orderId % Constants.CUSTOMER_PER_DISTRICT;
					
			if(batchNumber < Constants.NEW_ORDER_PER_DISTRICT) {
				NewOrders newOrder = new NewOrders(txId, null);
				newOrder.setWarehouseId(warehouseId);
				newOrder.setDistrictId(districtId);
				newOrder.setOrderId(orderId);
				
				records.add(newOrder);
			}
			
			Orders order = new Orders(txId, null);
			order.setAllLocal((short) 1);
			order.setCarrierId(carrierId);
			order.setCustomerId(customerId);
			order.setDistrictId(districtId);
			order.setWarehouseId(warehouseId);
			order.setEntryDate(now);
			order.setOrderId(orderId);
			order.setOrderLineCount(orderLineCount);
			
			records.add(order);
			
			for(short j = 0; j < orderLineCount; j++) {
				OrderLine orderLine = new OrderLine(txId, null);
				orderLine.setOrderId(orderId);
				orderLine.setDistrictId(districtId);
				orderLine.setWarehouseId(warehouseId);
				orderLine.setLineNumber(j);
				orderLine.setItemId((short) randomInt(0, Constants.ITEM_NUMBER));
				orderLine.setWarehouseId(warehouseId);
				orderLine.setDistInfo(randomString(24, 24));
				orderLine.setQuantity( (short) 5 );
				
				BigDecimal amount;
				if(orderId > 2100) {
					amount = new BigDecimal((float) 0.0);
				}
				else {
					orderLine.setDeliveryDate(now);
					amount = randomFloat(10, 10000);
				}
				orderLine.setAmount(amount);
				
				records.add(orderLine);
			}
		}
/*		
		if(LOGGER.isInfoEnabled()) {
			LOGGER.info(
				"Order number {} / {}",
				batchNumber, 
				numberOfBatches()
			);
		}
*/
		return records;
	}

}
