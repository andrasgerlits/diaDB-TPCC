package com.dianemodb.tpcc.query.neworder;

import java.util.List;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Stock;
import com.dianemodb.tpcc.schema.StockTable;

public class FindStockByItemAndWarehouseId extends SingleIndexQueryDistributionPlan<Stock> {

	public static final String ID = "FindStockByItemAndWarehouseId";
	
	private static final String QUERY = 
			"SELECT * FROM " + StockTable.TABLE_NAME 
			+ " WHERE " + StockTable.ITEM_ID_COLUMN_NAME + "=? "
					+ "AND " + StockTable.WAREHOUSE_ID_COLUMN_NAME+ "=?";
	
	public FindStockByItemAndWarehouseId(StockTable table) {
		super(
			ID, 
			QUERY, 
			table, 
			List.of(
				StockTable.ITEM_ID_COLUMN, 
				StockTable.WAREHOUSE_ID_COLUMN
			)
		);
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}

}
