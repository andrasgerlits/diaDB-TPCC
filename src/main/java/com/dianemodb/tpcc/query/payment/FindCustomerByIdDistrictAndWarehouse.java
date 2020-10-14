package com.dianemodb.tpcc.query.payment;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByIdDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerById";

	public FindCustomerByIdDistrictAndWarehouse(CustomerTable table) {
		super(
			ID, 
			"SELECT * FROM " + CustomerTable.TABLE_NAME
			+ " WHERE " + CustomerTable.WAREHOUSE_ID_COLUMN_NAME + "=?"
				+ " AND " + CustomerTable.DISTRICT_ID_COLUMN_NAME + "=?" 
				+ " AND " + CustomerTable.ID_COLUMN_NAME + "=?",
			table, 
			table.getCompositeIndex()
		);
	}

}
