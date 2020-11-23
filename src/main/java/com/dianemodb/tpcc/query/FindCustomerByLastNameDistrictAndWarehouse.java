package com.dianemodb.tpcc.query;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByLastNameDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerByLastName";

	public FindCustomerByLastNameDistrictAndWarehouse(CustomerTable table) {
		// warehouse, district, last-name
		super(ID, table, table.getLastNameIndex());
	}
}
