package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Random;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.tpcc.query.FindOrderLinesByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.orderstatus.FindMaxOrderIdForCustomer;

public class OrderStatus extends TpccTestProcess {

	private final CustomerSelectionStrategy customerSelectionStrategy;
	
	private final byte districtId;
	
	public OrderStatus(
			Random random,
			ServerComputerId txComputer, 
			SQLServerApplication application, 
			short warehouseId,
			byte districtId,
			String uuid
	) {
		super(random, application, txComputer, 2000, 10000, warehouseId, uuid);
		this.districtId = districtId;
		this.customerSelectionStrategy = randomStrategy(random, warehouseId, districtId);
	}
	
	@Override
	protected Result startTx() {
		Envelope customerQuery = customerSelectionStrategy.customerQuery(this);
		return of(customerQuery, this::processCustomer);
		
	}

	private Result processCustomer(Object result) {
		RecordWithVersion<Customer> customer = 
				customerSelectionStrategy.getCustomerFromResult(result);
		
		Envelope maxOrderIdQuery =
				query(
					FindMaxOrderIdForCustomer.ID, 
					List.of(terminalWarehouseId, districtId, customer.getRecord().getPublicId())
				);
		
		return of(maxOrderIdQuery, r -> this.update(r, customer));
	}

	private Result update(Object results, RecordWithVersion<Customer> customerRecord) {
		RecordWithVersion<Orders> order = TpccTestProcess.singleFromResultList(results);
		int orderId = order.getRecord().getOrderId();
		
		Envelope findOrderLinesQuery = 
				query(
					FindOrderLinesByWarehouseDistrictOrderId.ID,
					List.of(terminalWarehouseId, districtId, orderId)
				);
		
		// if something needs to be done with the resulting info, override "commit()"
		return of(List.of(findOrderLinesQuery), this::commit);
	}
}
