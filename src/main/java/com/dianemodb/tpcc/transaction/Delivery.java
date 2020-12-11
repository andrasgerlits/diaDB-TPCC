package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.FindOrderLinesByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.delivery.FindNewOrderWithLowestOrderIdByWarehouseAndDistrict;
import com.dianemodb.tpcc.query.delivery.FindOrderByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.payment.FindCustomerByWarehouseDistrictAndId;

public class Delivery extends TpccTestProcess {

	private static final Logger LOGGER = LoggerFactory.getLogger(Delivery.class.getName());

	private final short carrierId; 
	
	private List<RecordWithVersion<NewOrders>> newOrders;
	
	protected Delivery(
			Random random,
			ServerComputerId txComputer,
			SQLServerApplication application, 
			short warehouseId,
			String uuid
	) {
		super(random, application, txComputer, 2000, 5000, warehouseId, uuid);
		this.carrierId = TpccDataInitializer.randomCarrierId();
	}

	@Override
	protected Result startTx() {
		LOGGER.debug("start {}", uuid);

		List<Envelope> queries =
				IntStream.range(0, Constants.DISTRICT_PER_WAREHOUSE)
					.mapToObj( 
						districtId -> 
							query(
								FindNewOrderWithLowestOrderIdByWarehouseAndDistrict.ID,
								List.of( terminalWarehouseId, Byte.valueOf( (byte) districtId) )
							)
					)
				.collect(Collectors.toList());
		
		return of(queries, this::process);
	}

	@SuppressWarnings("unchecked")
	private Result process(List<Object> r) {
		List<List<RecordWithVersion<NewOrders>>> results = (List) r;
		
		LOGGER.debug("Process {} {}", uuid, results);
		
		assert results.size() == 1 : results;

		// finish TX without writing anything to the DB if there was nothing in any one
		if(results.isEmpty() || results.stream().anyMatch( rl -> rl.isEmpty())) {
			return of(List.of(), this::commit);
		}
		
		List<RecordWithVersion<NewOrders>> orders = FunctionalUtil.flatten(results);
		
		// if there were no empty resultsets, this must match
		assert orders.size() == Constants.DISTRICT_PER_WAREHOUSE;
		
		 // select orders for each new_order found
		this.newOrders = (List<RecordWithVersion<NewOrders>>) results.iterator().next();

		LOGGER.debug("toQueries {} {}", uuid, newOrders);
		
		List<NewOrders> noList = 
				newOrders.stream()
					.map(RecordWithVersion::getRecord)
					.collect(Collectors.toList());
				
		List<List<?>> orderQueryParams = 
				new ArrayList<>(
					noList.stream()
						.map(this::toOrderParamList)
						.collect(Collectors.toSet())
				);
		
		Envelope orderQuery = query(FindOrderByWarehouseDistrictOrderId.ID, orderQueryParams );
		
		List<List<? extends Number>> customerQueryParams = 
				new ArrayList<>(
					noList.stream()
						.map(this::toCustomerParamList)
						.collect(Collectors.toSet())
				);
		
		Envelope customerQuery = query(FindCustomerByWarehouseDistrictAndId.ID, customerQueryParams);
		
		List<List<?>> orderLineQueryParams = 
				new ArrayList<>(
					noList.stream()
						.map(this::toOrderLineParamList)
						.collect(Collectors.toSet())
				);
		
		Envelope orderLinesQuery = query(FindOrderLinesByWarehouseDistrictOrderId.ID, orderLineQueryParams);

		return of(
				List.of(orderQuery, customerQuery, orderLinesQuery), 
				this::updateRecords
			);
	}

	private List<? extends Number> toCustomerParamList(NewOrders no) {
		return List.of(terminalWarehouseId, no.getDistrictId(), no.getCustomerId());
	}
	
	private List<Number> toOrderParamList(NewOrders no) {
		return List.of(terminalWarehouseId, no.getDistrictId(), no.getOrderId());
	}

	private List<Number> toOrderLineParamList(NewOrders no) {
		return List.of(no.getWarehouseId(), no.getDistrictId(), no.getOrderId());
	}

	private <U extends UserRecord, A, R> Map<NewOrders, R> toNewOrderMap(
			Function<NewOrders, List<?>> paramListFunction,
			List<RecordWithVersion<U>> records,
			Collector<RecordWithVersion<U>, A, Map<List<?>, R>> collector
	) {
		Map<List<?>, NewOrders> newOrdersByList = 
				newOrders.stream()
					.map(RecordWithVersion::getRecord)
					.collect(
						Collectors.toMap(
							paramListFunction, 
							Function.identity()
						)
					);
		
		Map<List<?>, R> recordsByList = records.stream().collect(collector);
		
		return newOrdersByList.entrySet()
					.stream()
					.collect( 
						Collectors.toMap(
							e -> e.getValue(), 
							e -> recordsByList.get(e.getKey())
						)
					);		
	}
	
	private <U extends UserRecord> Map<NewOrders, RecordWithVersion<U>> toNewOrderMap(
			Function<NewOrders, List<?>> paramListFunction,
			Function<U, List<?>> recordListFunction,
			List<RecordWithVersion<U>> records
	) {
		return toNewOrderMap(
				paramListFunction, 
				records, 
				Collectors.toMap(
					rwv -> recordListFunction.apply(rwv.getRecord()), 
					Function.identity()
				)
			);
	}

	private Result updateRecords(List<? extends Object> results) {
		LOGGER.debug("update {} {}", uuid, results);
		Timestamp now = new Timestamp(System.currentTimeMillis());
		
		List<List<? extends RecordWithVersion<? extends Record>>> resultLists = 
				(List<List<? extends RecordWithVersion<? extends Record>>>) results;
		
		/*
		 * associate new-order records to their matching pair(s), by creating
		 * the same set of attributes from both entities which was used to query
		 * them from the new-order record.
		 */
		
		Iterator<List<? extends RecordWithVersion<? extends Record>>> listIterator = resultLists.iterator();
		
		List<RecordWithVersion<Orders>> orderList = (List<RecordWithVersion<Orders>>) listIterator.next();
		
		Map<NewOrders, RecordWithVersion<Orders>> ordersByNewOrders = 
				toNewOrderMap(
					this::toOrderParamList, 
					o -> List.of(o.getWarehouseId(), o.getDistrictId(), o.getOrderId()), 
					orderList
				);
		
		List<RecordWithVersion<Customer>> customerList = 
				(List<RecordWithVersion<Customer>>) listIterator.next();
		
		Map<NewOrders, RecordWithVersion<Customer>> customersByNewOrders = 
				toNewOrderMap(
					this::toCustomerParamList, 
					c -> List.of(c.getWarehouseId(), c.getDistrictId(), c.getPublicId()), 
					customerList
				);
		
		List<RecordWithVersion<OrderLine>> orderLineList = 
				(List<RecordWithVersion<OrderLine>>) listIterator.next();

		// there will be multiple order-lines for each new-order, so they need to be grouped
		Map<NewOrders, List<RecordWithVersion<OrderLine>>> orderLineByNewOrders = 
				toNewOrderMap(
					this::toOrderLineParamList,  
					orderLineList,
					Collectors.groupingBy(
						rwv -> {
							OrderLine ol = rwv.getRecord();
							return List.of(ol.getWarehouseId(), ol.getDistrictId(), ol.getOrderId());
						}
					)						
				);
		
		// alternating order-customer pairs, listed in the order in "newOrders"
		Iterator<RecordWithVersion<NewOrders>> newOrdersIterator = newOrders.iterator();

		ModificationCollection modificationCollection = new ModificationCollection();
		while(newOrdersIterator.hasNext()) {
			RecordWithVersion<NewOrders> newOrder = newOrdersIterator.next();
			
			NewOrders newOrderRecord = newOrder.getRecord();
			
			RecordWithVersion<Orders> order = ordersByNewOrders.get(newOrderRecord);
			RecordWithVersion<Customer> customer = customersByNewOrders.get(newOrderRecord);
			
			List<RecordWithVersion<OrderLine>> orderLines = 
					(List<RecordWithVersion<OrderLine>>) orderLineByNewOrders.get(newOrderRecord);
			
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

		return of(List.of(modifyEvent(modificationCollection)), this::commit);
	}
}
