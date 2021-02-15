package com.dianemodb.tpcc;

public class Constants {

	// 200 -> 40GB
	public static final int NUMBER_OF_WAREHOUSES = 4;
	
	public static final int STOCK_PER_WAREHOUSE = 100000;
	public static final int ORDER_LINE_PER_WAREHOUSE = 300000;
	
	public static final int DISTRICT_PER_WAREHOUSE = 10;
	
	public static final int CUSTOMER_PER_DISTRICT = 30000 / DISTRICT_PER_WAREHOUSE;
	public static final int ORDER_PER_DISTRICT = 30000 / DISTRICT_PER_WAREHOUSE;
	public static final int HISTORY_PER_DISTRICT = 30000 / DISTRICT_PER_WAREHOUSE;
	public static final int NEW_ORDER_PER_DISTRICT = 9000 / DISTRICT_PER_WAREHOUSE;
	
	public static final int ORDER_LINE_PER_ORDER_MIN = 5;
	public static final int ORDER_LINE_PER_ORDER_MAX = 15;

	public static final int ITEM_NUMBER = 100000;
	
	public static final int MSG_MAX_WAIT_MS = 10;
	
	public static final String BRAND_SIGNAL_KEY = "ORIGINAL";
	
	public static final int TERMINAL_PER_WAREHOUSE = 10;
	
	public static final String[] LAST_NAMES = 
			{
				"BAR", 
				"OUGHT", 
				"ABLE", 
				"PRI", 
				"PRES", 
				"ESE", 
				"ANTI", 
				"CALLY", 
				"ATION ", 
				"EING"
			};
	
}