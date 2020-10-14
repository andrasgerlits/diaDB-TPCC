package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerDetailsByWarehouseAndId extends SingleIndexQueryDistributionPlan<Customer> {

	private static final String QUERY = 
			"SELECT * FROM " + CustomerTable.TABLE_NAME 
			+ " WHERE " + CustomerTable.WAREHOUSE_ID_COLUMN_NAME+ "=? "
					+ "AND " + CustomerTable.DISTRICT_ID_COLUMN_NAME + "=? "
					+ "AND " + CustomerTable.ID_COLUMN_NAME + "=?";

	public FindCustomerDetailsByWarehouseAndId(CustomerTable table) {
		super(
			"findCustomerDetails", 
			QUERY, 
			table, 
			table.getCompositeIndex()
		);
	}

}
