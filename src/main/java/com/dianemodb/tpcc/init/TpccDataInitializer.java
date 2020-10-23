package com.dianemodb.tpcc.init;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.AddressAndTaxUserBaseRecord;
import com.dianemodb.tpcc.entity.LocationBasedUserRecord;

public abstract class TpccDataInitializer {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TpccDataInitializer.class.getName());
	
	protected static String randomString(int minLength, int maxLength) {
		return ByteUtil.randomString(minLength, maxLength);
	}

	protected static BigDecimal randomFloat(int lower, int upper) {
		return new BigDecimal(randomInt(lower, upper)).divide(new BigDecimal(100));
	}

	protected static int randomInt(int lower, int upper) {
		return ByteUtil.randomInt(lower, upper);
	}
	
	protected static String randomStreet() {
		return randomCity();
	}

	protected static String randomState() {
		return randomString(2, 2);
	}

	protected static String randomCity() {
		return randomString(10, 20);
	}

	protected static String randomName() {
		return randomString(6, 10);
	}
	
	protected static String randomZip() {
		return randomString(9, 9);
	}
	
	public static short randomCarrierId() {
		return (short) randomInt(1,  10);
	}
	
	protected static String randomValue(String...string) {
		// pick a random value from the specified array
		return string[randomInt(0, string.length -1 )];
	}
	
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
	
	protected static void randomLocationValues(LocationBasedUserRecord record) {
		record.setCity(randomCity());
		record.setState(randomState());
		record.setStreet1(randomStreet());
		record.setStreet2(randomStreet());
		record.setZip(randomZip());		
	}
	
	protected static void randomValues(AddressAndTaxUserBaseRecord record) {
		randomLocationValues(record);
		
		record.setName(randomName());
		record.setTax(randomFloat(10, 20));
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
	
	public ModificationCollection process(TransactionId txId) {
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
		
		ModificationCollection modificationCollection  = createModificationCollection(txId, numberProcessed++);
		
		LOGGER.info(
				"{} {} / {} \t\t {}:{}:{}", 
				itemTypeName, 
				numberProcessed, 
				numberOfBatches(), 
				hours, 
				minutes, 
				seconds
		);
		
		return modificationCollection;
	}
	
	protected abstract ModificationCollection createModificationCollection(TransactionId txId, int batchNumber);
	
}
