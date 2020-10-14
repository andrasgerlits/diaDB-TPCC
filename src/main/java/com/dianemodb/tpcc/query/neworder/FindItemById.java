package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Item;
import com.dianemodb.tpcc.schema.ItemTable;

public class FindItemById extends SingleIndexQueryDistributionPlan<Item> {
	
	private static final String QUERY = 
			"SELECT * "
			+ "FROM " + ItemTable.TABLE_NAME 
			+ " WHERE " + ItemTable.ID_COLUMN_NAME + "=?";
	
	public static final String ID = "findItemById";

	public FindItemById(ItemTable table) {
		super(ID, QUERY, table, table.getIdIndex());
	}
}
