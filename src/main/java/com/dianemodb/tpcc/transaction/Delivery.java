package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.dianemodb.ModificationCollection;
import com.dianemodb.Record;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.UserRecord;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderidDistrictAndWarehouse;
import com.dianemodb.tpcc.query.delivery.FindNewOrderWithLowestOrderIdByDistrictAndWarehouse;
import com.dianemodb.tpcc.query.delivery.FindOrderByDistrictWarehouseOrderId;
import com.dianemodb.tpcc.query.payment.FindCustomerByIdDistrictAndWarehouse;

public class Delivery extends TpccTestProcess {

	private final short warehouseId;
	private final short carrierId;
	
	private List<RecordWithVersion<NewOrders>> newOrders; 
	
	protected Delivery(
			SQLServerApplication application, 
			ServerComputerId txComputer,
			short warehouseId,
			short carrierId
	) {
		super(application, txComputer);
		this.warehouseId = warehouseId;
		this.carrierId = carrierId;
	}


	@Override
	protected Result startTx() {
		List<Envelope> queries = 
			IntStream.range(0, Constants.DISTRICT_PER_WAREHOUSE)
				.mapToObj(
					i -> 						
						query(
							FindNewOrderWithLowestOrderIdByDistrictAndWarehouse.ID,
							List.of( (short) i, warehouseId, carrierId)
						)
				)
				.collect(Collectors.toList());
		
		return of(queries, this::process);
	}


	private Result process(List<? extends Object> results) {
		// select orders for each new_order found
		List<List<RecordWithVersion<NewOrders>>> resultsLists = (List<List<RecordWithVersion<NewOrders>>>) results;
		
		 this.newOrders = 
				resultsLists.stream()
					.flatMap(Collection::stream)
					.collect(Collectors.toList());
		
		 List<Envelope> orderQueries = 
				 newOrders.stream()
					.flatMap(
						rv -> {
							NewOrders no = rv.getRecord();
							
							Envelope orderQuery = 
								query(
									FindOrderByDistrictWarehouseOrderId.ID, 
									List.of(no.getDistrictId(), warehouseId, no.getOrderId())
								);
							
							Envelope customerQuery = 
								query(
									FindCustomerByIdDistrictAndWarehouse.ID, 
									List.of(no.getCustomerId(), no.getDistrictId(), warehouseId)
								);
							
							Envelope orderLinesQuery =
								query(
									FindOrderLinesByOrderidDistrictAndWarehouse.ID,
									List.of(no.getOrderId(), no.getDistrictId(), no.getWarehouseId())
								);
							
							return Stream.of(orderQuery, customerQuery, orderLinesQuery);
						}
					)
					.collect(Collectors.toList());

		 return of(orderQueries, this::updateRecords);
	}

	private Result updateRecords(List<? extends Object> results) {
		Timestamp now = new Timestamp(System.currentTimeMillis());
		
		List<List<? extends RecordWithVersion<? extends Record>>> resultLists = (List<List<? extends RecordWithVersion<? extends Record>>>) results;
		Iterator<List<? extends RecordWithVersion<? extends Record>>> listIterator = resultLists.iterator();
		
		// alternating order-customer pairs, listed in the order in "newOrders"
		Iterator<RecordWithVersion<NewOrders>> newOrdersIterator = newOrders.iterator();

		ModificationCollection modificationCollection = new ModificationCollection();
		while(newOrdersIterator.hasNext()) {
			RecordWithVersion<NewOrders> newOrder = newOrdersIterator.next();
			RecordWithVersion<Orders> order = (RecordWithVersion<Orders>) FunctionalUtil.singleResult(listIterator.next());
			RecordWithVersion<Customer> customer = (RecordWithVersion<Customer>) FunctionalUtil.singleResult(listIterator.next());
			
			List<RecordWithVersion<OrderLine>> orderLines = (List<RecordWithVersion<OrderLine>>) listIterator.next();
			
			// make sure that lines are processed as expected
			assert newOrder.getRecord().getDistrictId() == order.getRecord().getDistrictId()
					&& newOrder.getRecord().getWarehouseId() == order.getRecord().getWarehouseId()
					&& newOrder.getRecord().getCustomerId() == customer.getRecord().getPublicId()
					&& newOrder.getRecord().getOrderId() == order.getRecord().getOrderId()
					&& newOrder.getRecord().getDistrictId() == customer.getRecord().getDistrictId()
					&& newOrder.getRecord().getWarehouseId() == customer.getRecord().getWarehouseId();
			
			// delete from new-orders
			modificationCollection.addDelete(newOrders, application);

			Orders updatedOrder = order.getRecord().shallowClone(application, txId);
			updatedOrder.setCarrierId(carrierId);
			modificationCollection.addUpdate(order, updatedOrder);
			
			BigDecimal sum = new BigDecimal(0);
			for(RecordWithVersion<OrderLine> orderLine : orderLines) {
				OrderLine updatedOrderLine = orderLine.getRecord().shallowClone(application, txId);
				updatedOrderLine.setDeliveryDate(now);
				
				sum.add(updatedOrderLine.getAmount());
				
				modificationCollection.addUpdate(orderLine, updatedOrderLine);
			}
			
			Customer updatedCustomer = customer.getRecord().shallowClone(application, txId);
			updatedCustomer.setBalance(updatedCustomer.getBalance().add(sum));
			updatedCustomer.setDeliveryCnt((short) (updatedCustomer.getDeliveryCnt() + 1));
			
			modificationCollection.addUpdate(customer, updatedCustomer);
		}

		return of(
			List.of(modifyEvent(modificationCollection)), 
			this::commit
		);
	}

}