package com.dianemodb.tpcc.transaction.neworder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.metaschema.RecordAndTransaction;
import com.dianemodb.sql.SingleIndexedColumnQueryDistributionPlan;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;

public class FindCustomerDetailsById extends SingleIndexedColumnQueryDistributionPlan<Customer, Integer> {

	private static final String QUERY = 
			"SELECT * FROM " + CustomerTable.TABLE_NAME 
			+ " WHERE " + CustomerTable.PUBLIC_ID_COLUMN_NAME + "=?";

	public FindCustomerDetailsById(CustomerTable table) {
		super(
			"findCustomerDetails", 
			QUERY, 
			table, 
			CustomerTable.PUBLIC_ID_COLUMN
		);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> toIndexParams(List<?> queryParams) {
		return (List<Integer>) queryParams;
	}

	@Override
	public Multiplicity indexType() {
		return Multiplicity.UNIQUE;
	}

	@Override
	public Map<RecordAndTransaction, Customer> findRecords(
			Connection connection, 
			List<? extends Integer> parameters,
			List<RecordAndTransaction> recordsToFind
	) throws SQLException 
	{
		assert parameters.size() == 1;
		
		return executeSingleParamQuery(
				connection, 
				FunctionalUtil.singleResult(parameters), 
				QUERY, 
				this::readFromResultSet,
				(i, stmt) -> stmt.setInt(1, i)
		);
	}

	@Override
	protected Customer readFromResultSet(ResultSet rs) {
		return FunctionalUtil.doOrPropagate(() -> ((CustomerTable) table).customerFromResultSet(rs));
	}
}
