package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.h2impl.SingleIndexMultipleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Item;
import com.dianemodb.tpcc.schema.ItemTable;

public class FindItemById extends SingleIndexMultipleParameterSetQueryDistributionPlan<Item> {
	
	public static final String ID = "findItemById";

	public FindItemById(ItemTable table) {
		super(ID, table, table.getIdIndex());
	}
}
