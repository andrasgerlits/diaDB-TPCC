package com.dianemodb.tpcc.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ServerComputerId;
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

/**
 * Supplies new TPC-C transactions based on the specification.
 * */
public class TpccProcessMaker {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessMaker.class.getName());
	
	@FunctionalInterface
	private interface TpccProcessFactory {
		TpccTestProcess get(short warehouseId, byte districtId, String uuid);
	}
	
	private final Map<Short, List<TpccProcessFactory>> factoriesByWarehouse = new HashMap<>();
	
	private final List<TpccProcessFactory> prototypeList;

	public TpccProcessMaker(SQLServerApplication application) {
		// tx maintained on the same computer as the warehouse-record
		WarehouseTable serverTable = (WarehouseTable) application.getTableById(WarehouseTable.ID);
		
		Condition<Warehouse> equalsIdCondition = 
				Condition.andEqualsEach(List.of(WarehouseTable.ID_COLUMN));
				
		Function<Short, ServerComputerId> f = 
				w -> {
					Set<ServerComputerId> computers = 
						serverTable.maintainingComputerDecidingIndex()
							.getMaintainingComputer(equalsIdCondition , List.of(w));
					
					// in the current configuration, each warehouse lives on a single instance
					assert computers.size() == 1;
					return computers.iterator().next();
				};
		
		prototypeList = createFactoryPrototypeList(application, f);
		
		for(short i = 0; i < Constants.NUMBER_OF_WAREHOUSES; i++ ) {
			List<TpccProcessFactory> factoryList = new LinkedList<>(prototypeList);
			Collections.shuffle(factoryList);
			factoriesByWarehouse.put(i, factoryList);
		}
	}
	
	public TpccTestProcess createNextProcess(
			short warehouseId, 
			byte districtId, 
			int variance
	) {
		String uuid = ByteUtil.randomStringUUIDMostSignificant();
		List<TpccProcessFactory> factoryList = factoriesByWarehouse.get(warehouseId);
		if(factoryList.isEmpty()) {
			List<TpccProcessFactory> list = new LinkedList<>(prototypeList);
			Collections.shuffle(list);
			factoriesByWarehouse.put(warehouseId, list);
			
			factoryList = list;
		}
		
		TpccProcessFactory factory = factoryList.remove(0);
		TpccTestProcess process = factory.get(warehouseId, districtId, uuid);
		
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug(
				"Started process {} {}", 
				process.getWarehouseId(), 
				process.getClass().getSimpleName()
			);
		}
		return process;
	}
	
	private List<TpccProcessFactory> createFactoryPrototypeList(
			SQLServerApplication application,
			Function<Short, ServerComputerId> f
	) {
		Random random = new Random();
		
		List<Pair<Integer, TpccProcessFactory>> factories = 
				List.of(
					Pair.of(10, (w, d, v) -> new NewOrder(random, f.apply(w), application, w, d, v)),
					Pair.of(10, (w, d, v) -> new Payment(random, f.apply(w), application, w, d, v)),
					Pair.of(1, (w, d, v) -> new OrderStatus(random, f.apply(w), application, w, d, v)),
					Pair.of(1, (w, d, v) -> new Delivery(random, f.apply(w), application, w, v)),
					Pair.of(1, (w, d, v) -> new StockLevel(random, f.apply(w), application, w, d, v))
				);
		
		List<TpccProcessFactory> prototypeList = new LinkedList<>();
		for(Pair<Integer, TpccProcessFactory> pair : factories) {
			for(int i=0; i < pair.getKey(); i++) {
				prototypeList.add(pair.getValue());
			}
		}
		return prototypeList;
	}
}
