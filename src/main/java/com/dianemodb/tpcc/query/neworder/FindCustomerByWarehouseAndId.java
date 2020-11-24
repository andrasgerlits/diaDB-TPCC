package com.dianemodb.tpcc.query.neworder;

import com.dianemodb.sql.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByWarehouseAndId extends SingleIndexSingleParameterSetQueryDistributionPlan<Customer> {

	public FindCustomerByWarehouseAndId(CustomerTable table) {
		// warehouse, district, id
		super("findCustomerDetails", table, table.getCompositeIndex());
	}

}
