package com.dianemodb.tpcc.query.payment;

import com.dianemodb.h2impl.MultipleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByWarehouseDistrictAndId extends MultipleParameterSetQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerById";

	public FindCustomerByWarehouseDistrictAndId(CustomerTable table) {
		// warehouse, district, id
		super(ID, table, table.getCompositeIndex());
	}

}
