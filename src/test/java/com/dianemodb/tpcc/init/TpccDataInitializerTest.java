package com.dianemodb.tpcc.init;

import org.junit.Assert;
import org.junit.Test;

import com.dianemodb.tpcc.Constants;

public class TpccDataInitializerTest {
	
	@Test
	public void testCalculatePositions() {
		int[][] positions = TpccDataInitializer.calculatePositions(1234, 10, 10, 10);
		
		Assert.assertEquals(1, positions[0][0]);
		Assert.assertEquals(2, positions[1][0]);
		Assert.assertEquals(3, positions[2][0]);
		
		Assert.assertEquals(4, positions[2][1]);
		
		positions = TpccDataInitializer.calculatePositions(
						Constants.CUSTOMER_PER_DISTRICT + 10, 
						Constants.DISTRICT_PER_WAREHOUSE,
						Constants.CUSTOMER_PER_DISTRICT
					);
		
		// 1st warehouse
		Assert.assertEquals(0, positions[0][0]);
		
		// 2nd district
		Assert.assertEquals(1, positions[1][0]);
		
		// customer-id
		Assert.assertEquals(10, positions[1][1]);

		positions = TpccDataInitializer.calculatePositions(
				Constants.CUSTOMER_PER_DISTRICT * Constants.DISTRICT_PER_WAREHOUSE * 5, 
				Constants.DISTRICT_PER_WAREHOUSE,
				Constants.CUSTOMER_PER_DISTRICT
			);

		// 5st warehouse
		Assert.assertEquals(5, positions[0][0]);
		
		// 1st district
		Assert.assertEquals(0, positions[1][0]);
		
		// customer-id
		Assert.assertEquals(0, positions[1][1]);

		positions = TpccDataInitializer.calculatePositions(
				Constants.CUSTOMER_PER_DISTRICT * Constants.DISTRICT_PER_WAREHOUSE * 5 + 1, 
				Constants.DISTRICT_PER_WAREHOUSE,
				Constants.CUSTOMER_PER_DISTRICT
			);

		// 5st warehouse
		Assert.assertEquals(5, positions[0][0]);
		
		// 1st district
		Assert.assertEquals(0, positions[1][0]);
		
		// customer-id
		Assert.assertEquals(1, positions[1][1]);
	}

}
