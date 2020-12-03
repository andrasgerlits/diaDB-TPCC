package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.Record;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.FindOrderLinesByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.delivery.FindNewOrderWithLowestOrderIdByWarehouseAndDistrict;
import com.dianemodb.tpcc.query.delivery.FindOrderByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.payment.FindCustomerByWarehouseDistrictAndId;

public class Delivery extends TpccTestProcess {

	private static final Logger LOGGER = LoggerFactory.getLogger(Delivery.class.getName());

	private final short carrierId;
	private final byte districtId; 
	
	private List<RecordWithVersion<NewOrders>> newOrders;
	
	protected Delivery(
			Random random,
			ServerComputerId txComputer,
			SQLServerApplication application, 
			short warehouseId,
			byte districtId,
			String uuid
	) {
		super(random, application, txComputer, 2000, 5000, warehouseId, uuid);
		this.carrierId = TpccDataInitializer.randomCarrierId();
		this.districtId = districtId;
	}

	@Override
	protected Result startTx() {
		LOGGER.debug("start {}", uuid);

		Envelope query = 						
				query(
					FindNewOrderWithLowestOrderIdByWarehouseAndDistrict.ID,
					List.of( terminalWarehouseId, districtId )
				);
		
		return of(query, this::process);
	}

	@SuppressWarnings("unchecked")
	private Result process(Optional<Object> result) {
		LOGGER.debug("Process {} {}", uuid, result);

		// finish TX without writing anything to the DB if there was nothing
		if(result.isEmpty()) {
			return of(List.of(), this::commit);
		}
		
		 // select orders for each new_order found
		this.newOrders = (List<RecordWithVersion<NewOrders>>) result.get();
		
		// or if there were fewer than 10
		if(newOrders.size() < 10) {
			return of(List.of(), this::commit);
		}
		 
		List<Envelope> orderQueries = 
				newOrders.stream()
					.flatMap( this::toQueries )
					.collect(Collectors.toList());

		return of(orderQueries, this::updateRecords);
	}

	private Stream<Envelope> toQueries(RecordWithVersion<NewOrders> rv) {
		LOGGER.debug("toQueries {} {}", uuid, rv);
		NewOrders no = rv.getRecord();
		
		Envelope orderQuery = 
			query(
				FindOrderByWarehouseDistrictOrderId.ID, 
				List.of(terminalWarehouseId, no.getDistrictId(), no.getOrderId())
			);
		
		Envelope customerQuery = 
			query(
				FindCustomerByWarehouseDistrictAndId.ID, 
				List.of(terminalWarehouseId, no.getDistrictId(), no.getCustomerId())
			);
		
		Envelope orderLinesQuery =
			query(
				FindOrderLinesByWarehouseDistrictOrderId.ID,
				List.of(no.getWarehouseId(), no.getDistrictId(), no.getOrderId())
			);
		
		return Stream.of(orderQuery, customerQuery, orderLinesQuery);
	}

	private Result updateRecords(List<? extends Object> results) {
		LOGGER.debug("update {} {}", uuid, results);
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
