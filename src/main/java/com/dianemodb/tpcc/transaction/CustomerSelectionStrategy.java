package com.dianemodb.tpcc.transaction;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.message.Envelope;
import com.dianemodb.tpcc.entity.Customer;

public interface CustomerSelectionStrategy {
		
	public RecordWithVersion<Customer> getCustomerFromResult(Object next);
	
	public Envelope customerQuery(TpccTestProcess process);

}
