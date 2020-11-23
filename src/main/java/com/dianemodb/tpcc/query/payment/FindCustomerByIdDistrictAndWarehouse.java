package com.dianemodb.tpcc.query.payment;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByIdDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerById";

	public FindCustomerByIdDistrictAndWarehouse(CustomerTable table) {
		// warehouse, district, id
		super(ID, table, table.getCompositeIndex());
	}

}
