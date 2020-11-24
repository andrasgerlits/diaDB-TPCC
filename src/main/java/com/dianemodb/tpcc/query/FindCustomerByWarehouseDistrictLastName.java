package com.dianemodb.tpcc.query;

import com.dianemodb.sql.SingleIndexSingleParameterSetQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByWarehouseDistrictLastName extends SingleIndexSingleParameterSetQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerByLastName";

	public FindCustomerByWarehouseDistrictLastName(CustomerTable table) {
		// warehouse, district, last-name
		super(ID, table, table.getLastNameIndex());
	}
}
