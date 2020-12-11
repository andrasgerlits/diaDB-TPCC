package com.dianemodb.tpcc.query;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.message.Envelope;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.query.payment.FindCustomerByWarehouseDistrictAndId;
import com.dianemodb.tpcc.transaction.TpccTestProcess;

public class CustomerSelectionById implements CustomerSelectionStrategy {

	private final int customerId;
	private final short warehouseId;
	private final byte districtId;
	
	public CustomerSelectionById(
			short warehouseId, 
			byte districtId,
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
		List<List<?>> customerParams = new LinkedList<>();
		customerParams.add(new ArrayList<>(List.of(warehouseId, districtId, customerId)));
		
		return TpccTestProcess.query(
				FindCustomerByWarehouseDistrictAndId.ID, 
				customerParams,
				process
			);
	}

	@Override
	public Optional<Integer> maybeCustomerId() {
		return Optional.of(customerId);
	}

}
