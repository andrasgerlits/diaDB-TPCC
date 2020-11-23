package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerDetailsByWarehouseAndId extends SingleIndexQueryDistributionPlan<Customer> {

	public FindCustomerDetailsByWarehouseAndId(CustomerTable table) {
		// warehouse, district, id
		super("findCustomerDetails", table, table.getCompositeIndex());
	}

}
