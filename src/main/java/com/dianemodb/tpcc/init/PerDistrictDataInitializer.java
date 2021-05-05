package com.dianemodb.tpcc.init;

import java.util.Arrays;

import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;

public abstract class PerDistrictDataInitializer extends TpccDataInitializer {

	final int[] numberInBatch;
	
	public PerDistrictDataInitializer(DianemoApplication application, int numberPerDistrict) {
		super(application);
		this.numberInBatch = 
				new int[]{ 
						Constants.DISTRICT_PER_WAREHOUSE, 
						numberPerDistrict
				};
	}

	protected byte getDistrictId(int offset) {
		int[][] numbers = calculatePositions(offset, numberInBatch);
		byte districtId = (byte) numbers[1][0];
		
		assert districtId < Constants.DISTRICT_PER_WAREHOUSE : offset + " " + Arrays.toString(numberInBatch) + " " + districtId;
		return districtId;
	}
	
	protected short getWarehouseId(int offset) {
		int[][] numbers =  calculatePositions(offset, numberInBatch);
		short warehouseId = (short) numbers[0][0];
		
		assert warehouseId < Constants.NUMBER_OF_WAREHOUSES : offset + " " + Arrays.toString(numberInBatch) + " " + warehouseId;
		return warehouseId;
	}

	protected int getRecordId(int offset) {
		int[][] numbers =  calculatePositions(offset, numberInBatch);
		short recordId = (short) numbers[1][1];
		
		assert recordId < numberInBatch[1] : offset + " " + Arrays.toString(numberInBatch) + " " + recordId;
		return recordId;
	}

}
