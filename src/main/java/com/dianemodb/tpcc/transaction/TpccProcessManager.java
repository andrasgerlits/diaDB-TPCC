package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess.NextStep;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.CustomerSelectionById;
import com.dianemodb.tpcc.query.CustomerSelectionByLastName;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;

public class TpccProcessManager extends ProcessManager {
	
	private abstract class TpccProcessFactory {
		abstract TpccTestProcess get(short warehouseId, byte districtId);
	}
	
	private class PaymentFactory extends TpccProcessFactory {

		@Override
		public TpccTestProcess get(short warehouseId, byte districtId) {
			return new Payment(random, randomComputer(), application, warehouseId, districtId);
		}
	}
	
	private class OrderStatusFactory extends TpccProcessFactory {

		@Override
		public TpccTestProcess get(short warehouseId, byte districtId) {
			return new OrderStatus(random, randomComputer(), application, warehouseId, districtId);
		}
		
	}
	
	private class DeliveryFactory extends TpccProcessFactory {

		@Override
		public Delivery get(short warehouseId, byte districtId) {
			return new Delivery(
						random,
						application, 
						randomComputer(), 
						warehouseId, 
						districtId
					);
		}
		
	}
	
	private class StockLevelFactory extends TpccProcessFactory {

		@Override
		public TpccTestProcess get(short warehouseId, byte districtId) {
			return new StockLevel(
						random,
						application, 
						randomComputer(), 
						warehouseId, 
						districtId
					);
		}
	}
	
	private class NewOrderFactory extends TpccProcessFactory {
		
		@Override
		public TpccTestProcess get(short warehouseId, byte districtId) {
			return new NewOrder(
						random,
						randomComputer(), 
						application, 
						warehouseId, 
						districtId
					);
		}
	}
	
	private final List<Pair<Integer, TpccProcessFactory>> factoryByWeight;
	
	private final int sum;
	
	public TpccProcessManager(
			SQLServerApplication application, 
			List<ServerComputerId> leafComputers
	) {
		super(application, leafComputers);
		
		this.factoryByWeight = 
				List.of(
						Pair.of(10, new NewOrderFactory()),
						Pair.of(10, new PaymentFactory()),
						Pair.of(1, new OrderStatusFactory()),
						Pair.of(1, new DeliveryFactory()),
						Pair.of(1, new StockLevelFactory())
				);
		
		this.sum = factoryByWeight.stream().mapToInt(Pair::getKey).sum();
	}
	
	@Override
	protected void failed(ConversationId conversationId, Throwable ex) {
		// this is an expected failure, so something like concurrent commit, so safe to retry
		
	}

	@Override
	protected NextStep nextProcess() {
		ServerComputerId txComputer = randomComputer();
		int processId = getRandom().nextInt(sum) + 1;
		
		Iterator<Pair<Integer, TpccProcessFactory>> iter = factoryByWeight.iterator();
		int sofar = 0;
		while(iter.hasNext()) {
			Pair<Integer, TpccProcessFactory> next = iter.next();
			sofar += next.getKey();
			
			if(sofar >= processId) {
				byte districtId = (byte) getRandom().nextInt(Constants.DISTRICT_PER_WAREHOUSE);
				short warehouseId = activeWarehouseIds.remove(0);
				
				TpccProcessFactory factory = next.getValue();
				TpccTestProcess process = factory.get(warehouseId, districtId);
				
				return process.start();
			}
		}
		
		throw new IllegalStateException();
	}

	@Override
	public int numberFinished() {
		return finished;
	}

	@Override
	protected void success() {
		
	}

}
