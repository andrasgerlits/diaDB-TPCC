package com.dianemodb.tpcc.query.neworder;

import java.util.List;

import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Item;
import com.dianemodb.tpcc.schema.ItemTable;

public class FindItemById extends SingleIndexQueryDistributionPlan<Item> {
	
	private static final String QUERY = 
			"SELECT * "
			+ "FROM " + ItemTable.TABLE_NAME 
			+ " WHERE " + ItemTable.ID_COLUMN_NAME + "=?";
	
	public static final String ID = "findItemById";

	public FindItemById(UserRecordTable<Item> table) {
		super(ID, QUERY, table, List.of(ItemTable.PUBLIC_ID_COLUMN));
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}
}
