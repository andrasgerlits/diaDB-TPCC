package com.dianemodb.tpcc.query;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.QueryDefinition;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.Operator;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.schema.CustomerTable;
import com.dianemodb.tpcc.transaction.TpccTestProcess;

public class CustomerSelectionByLastName implements CustomerSelectionStrategy {

	private final String lastName; 
	private final short warehouseId; 
	private final byte districtId;
	
	public CustomerSelectionByLastName(
			String lastName, 
			short warehouseId, 
			byte districtId
	) {
		this.lastName = lastName;
		this.warehouseId = warehouseId;
		this.districtId = districtId;
	}
	
	@Override
	public RecordWithVersion<Customer> getCustomerFromResult(Object next) {
		@SuppressWarnings("unchecked")
		List<RecordWithVersion<Customer>> customers = (List<RecordWithVersion<Customer>>) next;
		
		int customerIndex = customers.size() / 2;
		// find midpoint customer
		if(customers.size() % 2 == 0) {
			customerIndex = customerIndex - 1;
		}
		return customers.get(0);
	}

	@Override
	public Envelope customerQuery(TpccTestProcess process) {
		return TpccTestProcess.query(
				new QueryDefinition<>(
						CustomerTable.ID, 
						new Condition<>(
							List.of(
								Pair.of(CustomerTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
								Pair.of(CustomerTable.DISTRICT_ID_COLUMN, Operator.EQ),
								Pair.of(CustomerTable.LAST_NAME_COLUMN, Operator.EQ)
							)
						), 
						false
				), 
				List.of(warehouseId, districtId, lastName),
				process
			);
	}

	@Override
	public Optional<Integer> maybeCustomerId() {
		return Optional.empty();
	}
}
