package com.dianemodb.tpcc.query;

import java.util.List;

import com.dianemodb.sql.SingleIndexQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerByLastNameDistrictAndWarehouse extends SingleIndexQueryDistributionPlan<Customer> {

	public static final String ID = "findCustomerByLastName";

	public FindCustomerByLastNameDistrictAndWarehouse(CustomerTable table) {
		super(
			ID, 
			"SELECT * FROM " + CustomerTable.TABLE_NAME 
				+ " WHERE " + CustomerTable.LAST_NAME_COLUMN_NAME + "=? "
					+ "AND " + CustomerTable.WAREHOUSE_ID_COLUMN_NAME + "=? "
					+ "AND " + CustomerTable.DISTRICT_ID_COLUMN_NAME + "=?", 
			table, 
			List.of(
				CustomerTable.LAST_NAME_COLUMN, 
				CustomerTable.WAREHOUSE_ID_COLUMN, 
				CustomerTable.DISTRICT_ID_COLUMN
			)
		);
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.DISCRETE;
	}
}
