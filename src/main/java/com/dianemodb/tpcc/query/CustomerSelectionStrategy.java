package com.dianemodb.tpcc.query;

import java.util.Optional;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.message.Envelope;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.transaction.TpccTestProcess;

public interface CustomerSelectionStrategy {
		
	public RecordWithVersion<Customer> getCustomerFromResult(Object next);
	
	public Envelope customerQuery(TpccTestProcess process);
	
	public Optional<Integer> maybeCustomerId();

}
