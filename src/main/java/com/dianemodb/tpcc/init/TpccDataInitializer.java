package com.dianemodb.tpcc.init;

import java.math.BigDecimal;

import com.dianemodb.ModificationCollection;
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.AddressAndTaxUserBaseRecord;
import com.dianemodb.tpcc.entity.LocationBasedUserRecord;

public abstract class TpccDataInitializer {

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

	public TpccDataInitializer(SQLServerApplication application) {
		this.application = application;		
	}
	
	public abstract int numberOfBatches();
	
	public boolean hasNext() {
		return numberProcessed < numberOfBatches();
	}
	
	public ModificationCollection process(TransactionId txId) {
		return createModificationCollection(txId, numberProcessed++);
	}
	
	protected abstract ModificationCollection createModificationCollection(TransactionId txId, int batchNumber);
	
}
