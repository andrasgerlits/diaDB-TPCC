package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.dianemodb.UserRecord;
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.AddressAndTaxUserBaseRecord;
import com.dianemodb.tpcc.entity.LocationBasedUserRecord;

public abstract class TpccDataInitializer {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TpccDataInitializer.class.getName());
	
	public static String randomString(int minLength, int maxLength) {
		return ByteUtil.randomString(minLength, maxLength);
	}

	public static BigDecimal randomFloat(int lower, int upper) {
		return new BigDecimal(randomInt(lower, upper)).divide(new BigDecimal(100));
	}

	public static int randomInt(int lower, int upper) {
		return ByteUtil.randomInt(lower, upper);
	}
	
	public static String randomStreet() {
		return randomCity();
	}

	public static String randomState() {
		return randomString(2, 2);
	}

	public static String randomCity() {
		return randomString(10, 20);
	}

	public static String randomName() {
		return randomString(6, 10);
	}
	
	public static String randomZip() {
		return randomString(4, 4) + "11111";
	}
	
	public static short randomCarrierId() {
		return (short) randomInt(1,  10);
	}
	
	public static short randomWarehouseId() {
		return (short) randomInt(0, Constants.NUMBER_OF_WAREHOUSES);
	}
	
	public static int randomCustomerId() {
		return randomInt(0, Constants.CUSTOMER_PER_DISTRICT);
	}
	
	public static int randomItemId() {
		return randomInt(0, Constants.ITEM_NUMBER);
	}
	
	public static String randomValue(String...string) {
		// pick a random value from the specified array
		return string[randomInt(0, string.length -1 )];
	}
	
	/**
	 * <p>
	 * Using the known number of records on each parent-level, returns the id of 
	 * the parent as the first parameter, with 0 being the highest parent and the 
	 * offset since the start of the current parent as the second param.
	 * */
	public static int[][] calculatePositions(int index, int...numberInBatch) {
		int[][] result = new int[numberInBatch.length + 1][];
		
		int[] positionValues = new int[numberInBatch.length];
		
		for (int i = 0; i < numberInBatch.length; i++) {
			positionValues[i] = numberInBatch[i];
			
			for(int j = i + 1; j < numberInBatch.length; j++) {
				positionValues[i] *= numberInBatch[j];				
			}
			
			int parentIdAtPosition = (int) Math.floorDiv(index, positionValues[i]);
			
			int sinceStartOfParent = index % positionValues[i];
			
			result[i] = new int[] { parentIdAtPosition, sinceStartOfParent };
			
			index = sinceStartOfParent;
		}
		
		return result;
	}
	
	public static void randomLocationValues(LocationBasedUserRecord record) {
		record.setCity(randomCity());
		record.setState(randomState());
		record.setStreet1(randomStreet());
		record.setStreet2(randomStreet());
		record.setZip(randomZip());		
	}
	
	public static void randomValues(AddressAndTaxUserBaseRecord record) {
		randomLocationValues(record);
		
		record.setName(randomName());
		record.setTax(randomFloat(10, 20));
	}
	
	public static String randomLastName() {
		int id = randomInt(0, 999);
		return generateLastName(id);
	}
	
	public static String generateLastName(int customerId) {
		String idString = String.valueOf(customerId);	
		String name = 
			idString.chars()
				.mapToObj( 
					c -> {
						int p = Integer.valueOf(String.valueOf( (char) c ));
						assert p >=0 && p < 10 : p + " " + c;
						return Constants.LAST_NAMES[p]; 
					}
				)
				.collect(Collectors.joining());
		
		return name;
	}
	
	protected final SQLServerApplication application;
	private int numberProcessed = 0;
	private long startTime = -1;
	private final String itemTypeName;

	public TpccDataInitializer(SQLServerApplication application) {
		this.application = application;
		this.itemTypeName = getClass().getSimpleName();
	}
	
	public abstract int numberOfBatches();
	
	public boolean hasNext() {
		return numberProcessed < numberOfBatches();
	}
	
	public List<UserRecord> process(TransactionId txId) {
		long hours = 0;
		long minutes = 0;
		long seconds = 0;
		
		if(startTime == -1) {
			startTime = System.currentTimeMillis();
		}
		else {
			long totalTime = System.currentTimeMillis() - startTime;
			float averageTime = totalTime / numberProcessed;
			
			long remaininNumber = (long) (averageTime * ( numberOfBatches() - numberProcessed ));
			
			hours = TimeUnit.MILLISECONDS.toHours(remaininNumber);
			minutes = TimeUnit.MILLISECONDS.toMinutes(remaininNumber) % 60;
			seconds = TimeUnit.MILLISECONDS.toSeconds(remaininNumber) % 60;			
		}
		
		List<UserRecord> records = createModificationCollection(txId, numberProcessed++);
		
		LOGGER.info(
				"{} {} / {} \t\t {}:{}:{}", 
				StringUtils.leftPad(itemTypeName, 10), 
				StringUtils.leftPad(String.valueOf(numberProcessed), 5), 
				StringUtils.leftPad(String.valueOf(numberOfBatches()), 5), 
				pad(hours), 
				pad(minutes), 
				pad(seconds)
		);
		
		return records;
	}
	protected String pad(Number seconds) {
		return StringUtils.leftPad(String.valueOf(seconds.intValue()), 2, '0');
	}

	
	protected abstract List<UserRecord> createModificationCollection(TransactionId txId, int batchNumber);

}
