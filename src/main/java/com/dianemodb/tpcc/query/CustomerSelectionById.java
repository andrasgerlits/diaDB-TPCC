package com.dianemodb.tpcc.query;

import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.message.Envelope;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.query.payment.FindCustomerByIdDistrictAndWarehouse;
import com.dianemodb.tpcc.transaction.TpccTestProcess;

public class CustomerSelectionById implements CustomerSelectionStrategy {

	private final int customerId;
	private final short warehouseId;
	private final short districtId;
	
	public CustomerSelectionById(
			short warehouseId, 
			short districtId,
			int customerId
	) {
		this.warehouseId = warehouseId;
		this.districtId = districtId;
		this.customerId = customerId;
	}

	// returns a single client, id is precise
	@Override
	public RecordWithVersion<Customer> getCustomerFromResult(Object next) {
		return TpccTestProcess.singleFromResultList(next);
	}

	@Override
	public Envelope customerQuery(TpccTestProcess process) {
		return TpccTestProcess.query(
				FindCustomerByIdDistrictAndWarehouse.ID, 
				List.of(customerId, districtId, warehouseId),
				process
			);
	}

}
