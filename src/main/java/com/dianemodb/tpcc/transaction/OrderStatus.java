package com.dianemodb.tpcc.transaction;

import com.dianemodb.ServerComputerId;
import com.dianemodb.metaschema.SQLServerApplication;

public class OrderStatus extends TpccTestProcess {

	private final CustomerSelectionStrategy customerSelectionStrategy;
	
	public OrderStatus(SQLServerApplication application, CustomerSelectionStrategy customerSelectionStrategy) {
		super(application);
		this.customerSelectionStrategy = customerSelectionStrategy;
	}

	@Override
	protected ServerComputerId txMaintainingComputer() {
		return null;
	}

	@Override
	protected Result startTx() {
		return null;
	}
}
