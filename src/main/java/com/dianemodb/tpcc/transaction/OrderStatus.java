package com.dianemodb.tpcc.transaction;

import java.util.Iterator;
import java.util.List;

import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderidDistrictAndWarehouse;
import com.dianemodb.tpcc.query.orderstatus.FindMaxOrderIdForCustomer;

public class OrderStatus extends TpccTestProcess {

	private final CustomerSelectionStrategy customerSelectionStrategy;
	
	private final short warehouseId;
	private final short districtId;
	private final int customerId;
	
	public OrderStatus(
			ServerComputerId txComputer, 
			SQLServerApplication application, 
			CustomerSelectionStrategy customerSelectionStrategy,
			short warehouseId,
			short districtId,
			int customerId
	) {
		super(application, txComputer);
		this.customerSelectionStrategy = customerSelectionStrategy;
		this.warehouseId = warehouseId;
		this.districtId = districtId;
		this.customerId = customerId;
	}

	@Override
	protected Result startTx() {
		Envelope customerQuery = customerSelectionStrategy.customerQuery(this);
		
		Envelope maxOrderIdQuery =
				query(
					FindMaxOrderIdForCustomer.ID, 
					List.of(warehouseId, districtId, customerId)
				);
		
		return of(List.of(customerQuery, maxOrderIdQuery), this::update);
	}

	private Result update(List<Object> results) {
		Iterator<Object> resultIter = results.iterator();
		
		RecordWithVersion<Customer> customer = 
				customerSelectionStrategy.getCustomerFromResult(resultIter.next());
		
		RecordWithVersion<Orders> order = 
				FunctionalUtil.singleResult(
						(List<RecordWithVersion<Orders>>)resultIter.next()
				);
		int orderId = order.getRecord().getOrderId();
		
		Envelope findOrderLinesQuery = 
				query(
					FindOrderLinesByOrderidDistrictAndWarehouse.ID,
					List.of(orderId, districtId, warehouseId)
				);
		
		// if something needs to be done with the resulting info, override "commit()"
		return of(List.of(findOrderLinesQuery), this::commit);
	}
	
}
