package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.sql.SingleIndexMultipleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Stock;
import com.dianemodb.tpcc.schema.StockTable;

public class FindStockByWarehouseItem extends SingleIndexMultipleParameterSetQueryDistributionPlan<Stock> {

	public static final String ID = "FindStockByItemAndWarehouseId";
	
	public FindStockByWarehouseItem(StockTable table) {
		super(
			ID, 
			table, 
			table.getItemWarehouseIndex()			
		);
	}	
}