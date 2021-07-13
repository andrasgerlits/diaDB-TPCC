package com.dianemodb.tpcc.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.QueryDefinition;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.Operator;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.tpcc.schema.OrdersTable;

public class OrderStatus extends TpccTestProcess {

	private final CustomerSelectionStrategy customerSelectionStrategy;
	
	private final byte districtId;
	
	public OrderStatus(
			Random random,
			ServerComputerId txComputer, 
			DianemoApplication application, 
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

	private Result processCustomer(Optional<Object> result) {
		assert result.isPresent();
		
		RecordWithVersion<Customer> customer = 
				customerSelectionStrategy.getCustomerFromResult(result.get());
		
		Envelope maxOrderIdQuery =
				query(
					new QueryDefinition<>(
						OrdersTable.ID, 
						new Condition<>(
							List.of(
								Pair.of(OrdersTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
								Pair.of(OrdersTable.DISTRICT_ID_COLUMN, Operator.EQ),
								Pair.of(OrdersTable.CUSTOMER_ID_COLUMN, Operator.EQ)
							)
						), 
						false
					), 
					List.of(terminalWarehouseId, districtId, customer.getRecord().getPublicId())
				);
		
		return of(maxOrderIdQuery, r -> this.update(r, customer));
	}

	private Result update(Optional<Object> results, RecordWithVersion<Customer> customerRecord) {
		RecordWithVersion<Orders> order = TpccTestProcess.singleFromResultList(results.get());
		int orderId = order.getRecord().getOrderId();
		
		Envelope findOrderLinesQuery = 
				query(
					Delivery.FIND_ORDER_LINES_BY_WH_DS_ID,
					List.of(new ArrayList<>(List.of(terminalWarehouseId, districtId, orderId)))
				);
		
		// if something needs to be done with the resulting info, override "commit()"
		return of(List.of(findOrderLinesQuery), this::commit);
	}
}
