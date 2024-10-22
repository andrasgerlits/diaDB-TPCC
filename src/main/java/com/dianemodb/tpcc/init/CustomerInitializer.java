package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.History;

public class CustomerInitializer extends PerDistrictDataInitializer {
	
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CustomerInitializer.class.getName());

	private static final int ITEM_PER_BATCH = 1000;
	
	private static final int TOTAL_NUMBER_OF_BATCHES = 
			Constants.NUMBER_OF_WAREHOUSES 
				* Constants.DISTRICT_PER_WAREHOUSE 
				* Constants.CUSTOMER_PER_DISTRICT 
				/ ITEM_PER_BATCH;

	public CustomerInitializer(DianemoApplication application) {
		super(application, Constants.CUSTOMER_PER_DISTRICT);
	}

	@Override
	public int numberOfBatches() {
		return TOTAL_NUMBER_OF_BATCHES;
	}

	@Override
	protected List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber) {
		List<UserRecord> records = new LinkedList<>();
		
		short warehouseId = -1;
		byte districtId = -1;
		int customerId = -1;
		for( short i = 0 ; i < ITEM_PER_BATCH; i++ ) {
			int totalOffset = batchNumber + (TOTAL_NUMBER_OF_BATCHES * i);
			
			warehouseId = getWarehouseId(totalOffset);
			districtId = getDistrictId(totalOffset);
			
			// number of index increments since start of district's id
			customerId = getRecordId(totalOffset);
			
			assert customerId <= Constants.CUSTOMER_PER_DISTRICT;
			
			Timestamp now = new Timestamp(System.currentTimeMillis());
			
			Customer customer = new Customer(txId, null);
			randomLocationValues(customer);
			customer.setPhone(randomString(16, 16));
			customer.setCredit(randomValue("B", "G") + "C");
			customer.setCreditLimit(50000);
			customer.setDiscount(randomFloat(0, 50));
			customer.setSince(now);
			customer.setFirstName(randomString(8, 16));
			customer.setMiddleName("OE");
			int lastNameSeed;
			if(customerId < 1000) {
				lastNameSeed = customerId;
			}
			else {
				lastNameSeed = randomInt(0, 999);
			}
			
			customer.setLastName(generateLastName(lastNameSeed));
			
			customer.setWarehouseId(warehouseId);
			customer.setDistrictId(districtId);
			customer.setPublicId(customerId);
			customer.setBalance(new BigDecimal(-10));
			customer.setYtdPayment(new BigDecimal(10));
			customer.setDeliveryCnt((short) 0);
			customer.setPaymentCnt((short) 1);
			customer.setData(randomString(300, 500));

			records.add(customer);
			
			History history = new History(txId, null);
			history.setCustomerId(customerId);
			history.setWarehouseId(warehouseId);
			history.setDistrictId(districtId);
			history.setCustomerWarehouseId(warehouseId);
			history.setCustomerDistrictId(districtId);
			history.setAmount(new BigDecimal(10));
			history.setData(randomString(12,24));
			history.setDate(now);
		
			records.add(history);
		}

		return records;
	}
}
