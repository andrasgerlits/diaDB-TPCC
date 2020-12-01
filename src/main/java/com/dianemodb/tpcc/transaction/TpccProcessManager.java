package com.dianemodb.tpcc.transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.exception.ClientInitiatedRollbackTransactionException;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.NextStep;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

import fj.data.Either;

public class TpccProcessManager extends ProcessManager {
	
	private static final int NUMBER_OF_TERMINALS = Constants.TERMINAL_PER_WAREHOUSE * Constants.NUMBER_OF_WAREHOUSES;


	@FunctionalInterface
	private interface TpccProcessFactory {
		TpccTestProcess get(short warehouseId, byte districtId, int variance);
	}
	
	private static final int TIME_MEASUREMENT_INTERVAL = 1 * 60 * 1000;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessManager.class.getName());
	
	private static final Logger TECH_LOGGER = LoggerFactory.getLogger(TpccClientRunner.class.getName());
	
	/**
	 * The pool of terminals, from which the 
	 * - first one is taken each time a new TX started 
	 * - the last one is appended each time a TX finished 
	 */
	private final List<Short> freeTerminals = new LinkedList<>();
	
	/**
	 * The number of processes which could be started (because we have more remaining and
	 * because we didn't hit the upper limit on the number of parallel TX), but which 
	 * couldn't for some reason known only to the specific implementation.
	 * */
	private int outstanding = 0;
	
	private long lastDumpTime = System.currentTimeMillis();

	/**
	 * In a single configured window, how many types of transactions were executed in 
	 * */
	private final Map<Long, Map<Class<? extends TpccTestProcess>, Integer>> processingTimeByTxType = 
			new HashMap<>();
	
	private final Map<Short, List<TpccProcessFactory>> factoriesByWarehouse = new HashMap<>();
	
	private final NavigableMap<Long, Short> terminalFreeupTimes = new TreeMap<>();
	
	private final List<NextStep> toRetry = new LinkedList<>();
	
	private final List<TpccProcessFactory> prototypeList;
	
	public TpccProcessManager(
			SQLServerApplication application, 
			List<ServerComputerId> leafComputers
	) {
		super(application, leafComputers);
		
		// tx maintained on the same computer as the warehouse-record
		WarehouseTable serverTable = (WarehouseTable) application.getTableById(WarehouseTable.ID);
		
		Condition<Warehouse> equalsIdCondition = 
				Condition.andEqualsEach(List.of(WarehouseTable.ID_COLUMN));
		
		Function<Short, ServerComputerId> f = 
				w -> {
					Set<ServerComputerId> computers = 
						serverTable.getMaintainingComputerDecidingIndex()
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
		
		for( short i = 0; i < NUMBER_OF_TERMINALS ; i++ ) {
			// 0, 1, 2 ... 0, 1, 2 ...
			freeTerminals.add( (short) (i % Constants.NUMBER_OF_WAREHOUSES) );
		}
		
		assert assertSlots();
		
		/*
		 * we can start a tx for each terminal right away, 
		 * start with an initial variance of 10 seconds to space out the first requests 
		 */
		startNewProcess(freeTerminals.size(), 0);
		
		assert assertSlots();
	}

	private List<TpccProcessFactory> createFactoryPrototypeList(
			SQLServerApplication application,
			Function<Short, ServerComputerId> f
	) {
		List<Pair<Integer, TpccProcessFactory>> factories = 
				List.of(
						Pair.of(10, (w, d, v) -> new NewOrder(random, f.apply(w), application, w, d, v)),
						Pair.of(10, (w, d, v) -> new Payment(random, f.apply(w), application, w, d, v)),
						Pair.of(1, (w, d, v) -> new OrderStatus(random, f.apply(w), application, w, d, v)),
						Pair.of(1, (w, d, v) -> new Delivery(random, f.apply(w), application, w, d, v)),
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
	
	@Override
	public synchronized boolean isFinished() {
		// Tick-tock, the party don't stop
		return false;
	}

	@Override
	protected Result failed(ConversationId conversationId, Throwable ex, TestProcess testProcess) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Process failed {}", testProcess);
			LOGGER.debug("", ex);
		}

		TpccTestProcess process = (TpccTestProcess) testProcess; 
		
		// if this failure was triggered by the client-API (so a designed failure)
		if(ex instanceof ClientInitiatedRollbackTransactionException) {
			// these are supposed to be finished transactions
			success(testProcess);
		}
		// if this was an expected diadb exception, like concurrent commit, so safe to retry
		else {
			// since this process was using a terminal anyway, we can simply restart it until it goes through
			toRetry.add(process.cancelAndRetry());
		}
		return TestProcess.FINISHED;
	}

	@Override
	protected List<NextStep> nextProcess(int number, int maxVarianceMs) {
		List<NextStep> result = new LinkedList<>();
		
		// if there was anything to retry, return that first
		while(!toRetry.isEmpty()) {
			NextStep process = toRetry.remove(0);
			process.getProcess().retry();
			result.add(process);

			if(result.size() == number) {
				return result;
			}
		}
		
		/*
		 * clean up potentially freed up terminals. The time for these is marked
		 * when the process has finished, but they're only marked as available just
		 * before they're needed.
		 */
		returnNoLongerUsedTerminals();
		
		while(!freeTerminals.isEmpty() && result.size() < number) {
			result.add(startNewProcess(maxVarianceMs));
		}
		
		return result;
	}
	
	private NextStep startNewProcess(int maxVarianceMs) {
		byte districtId = (byte) getRandom().nextInt(Constants.DISTRICT_PER_WAREHOUSE);
		short warehouseId = freeTerminals.remove(0);
		
		int variance;
		if(maxVarianceMs > 0) {
			variance = random.nextInt(maxVarianceMs);
		} 
		else {
			variance = 0;
		}
		
		TpccTestProcess process = createNextProcess(districtId, warehouseId, variance);
		
		NextStep nextStep = process.start();
		
		if(LOGGER.isDebugEnabled()) {
			// add as many warehouse-ids as there are terminals
			long now = System.currentTimeMillis();
			LOGGER.debug(
					"Started process warehouse {} wait {} ms {}", 
					warehouseId, 
					process.getInitialRequestStartTime() - now, 
					process.getClass().getSimpleName()
			);
		}

		return nextStep;
	}
	
	public void process(Map<ConversationId, Either<Object, ? extends Throwable>> results) {
		super.process(results);
		
		returnNoLongerUsedTerminals();
		
		int numberToStart = freeTerminals.size() + outstanding;
		// if nothing to start
		if(numberToStart == 0) {
			return;
		}
		
		int started = startNewProcess(numberToStart, 0);
		
		// if nothing was started
		if(started == 0) {
			return;
		}
		
		assert started <= numberToStart;
		
		outstanding = numberToStart - started;
		assert outstanding >= 0;
		
		if(LOGGER.isDebugEnabled() && (started > 0 || outstanding > 0)) {
			LOGGER.debug("Started {} processes, outstanding {}", started, outstanding);
		}
		
		processLoggedTimes();
	}

	private void processLoggedTimes() {
		long now;
		if((now = System.currentTimeMillis()) - lastDumpTime < TIME_MEASUREMENT_INTERVAL) {
			return;
		}
		
		Map<Class<? extends TpccTestProcess>, Integer> failures = new HashMap<>();
		Map<Class<? extends TpccTestProcess>, Integer> successes = new HashMap<>();
		
		// iterate from top to bottom
		NavigableSet<Long> timeslots = new TreeSet<>(processingTimeByTxType.keySet());
		while(!timeslots.isEmpty()) {
			long highest = timeslots.last();
			Map<Class<? extends TpccTestProcess>, Integer> m = processingTimeByTxType.get(highest);
			
			for(Map.Entry<Class<? extends TpccTestProcess>, Integer> e : m.entrySet()) {
				Class<? extends TpccTestProcess> c = e.getKey();
				int number = e.getValue();
				long whenLate = TpccTestProcess.MAX_TIMES_BY_CLASS.get(c);
				
				// report as late
				if(highest >= whenLate) {
					failures.putIfAbsent(c, 0);
					failures.put(c, failures.get(c) + number);
				}
				else {
					successes.putIfAbsent(c, 0);
					successes.put(c, successes.get(c) + number);				
				}
			}
			
			timeslots.remove(highest);
		}
		
		for(Entry<Class<? extends TpccTestProcess>, Integer> e : failures.entrySet()) {
			LOGGER.info("Failure {} {}", e.getKey().getSimpleName(), e.getValue());
		}
		
		for(Entry<Class<? extends TpccTestProcess>, Integer> e : successes.entrySet()) {
			LOGGER.info("Success {} {}", e.getKey().getSimpleName(), e.getValue());
		}
		
		TECH_LOGGER.debug("Total slots {}", totalSlots());
		
		TECH_LOGGER.debug("Free terminals {}", freeTerminals);
		TECH_LOGGER.debug(
				"freeUpTimes ms {}", 
				terminalFreeupTimes.keySet()
					.stream()
					.map(t -> t - now)
					.collect(Collectors.toList())
		);
		TECH_LOGGER.debug("Waiting {}", processesWaiting());

		TECH_LOGGER.debug("Outstanding {}", outstanding);
		TECH_LOGGER.debug("toRetry {}", toRetry);
		
		lastDumpTime = now;
		processingTimeByTxType.clear();
	}

	private int totalSlots() {
		// the ones in thinking time
		return  terminalFreeupTimes.size() 
			// the free ones
			+ freeTerminals.size() 
			// the ones being executed
			+ super.processesWaiting().size();
	}
	
	protected int startNewProcess(int number, int maxVarianceMs) {
		int result = super.startNewProcess(number, maxVarianceMs);
		
		assert assertSlots();
		return result;
	}

	private boolean assertSlots() {
		// we can have more, since StockLevel doesn't use a terminal, but is a running process
		assert totalSlots() >= NUMBER_OF_TERMINALS : 
				totalSlots() + "\n"  
				+ terminalFreeupTimes + "\n" 
				+ freeTerminals + "\n" 
				+ super.processesWaiting();
		
		return true;
	}

	private synchronized void returnNoLongerUsedTerminals() {
		// clean up the free terminals list, return the ones which are no longer used
		SortedMap<Long, Short> subMap = 
				new TreeMap<>(
					terminalFreeupTimes.subMap(0L, System.currentTimeMillis())
				);

		Collection<Short> terminals = subMap.values();
		if(LOGGER.isDebugEnabled() && !terminals .isEmpty()) {
			// add as many warehouse-ids as there are terminals
			LOGGER.debug("Freeing up terminals {}", terminals);
		}

		freeTerminals.addAll(terminals);
	
		// remove freeup times once they've been dealt with
		subMap.keySet().forEach( t -> terminalFreeupTimes.remove(t) );
		
		assert assertSlots();
	}
	
	private TpccTestProcess createNextProcess(
			byte districtId, 
			short warehouseId, 
			int variance
	) {
		List<TpccProcessFactory> factoryList = factoriesByWarehouse.get(warehouseId);
		if(factoryList.isEmpty()) {
			List<TpccProcessFactory> list = new LinkedList<>(prototypeList);
			Collections.shuffle(list);
			factoriesByWarehouse.put(warehouseId, list);
			
			factoryList = list;
		}
		
		TpccProcessFactory factory = factoryList.remove(0);
		TpccTestProcess process = factory.get(warehouseId, districtId, variance);
		return process;
	}
	

	@Override
	protected void success(TestProcess process) {
		TpccTestProcess tpccProcess = (TpccTestProcess) process;
		long latency = System.currentTimeMillis() - tpccProcess.getInitialRequestStartTime();

		LOGGER.info(
				"Process success {} {} {} {}", 
				StringUtils.leftPad(process.getClass().getSimpleName(), 15),
				StringUtils.leftPad(String.valueOf(latency), 10),
				StringUtils.leftPad(String.valueOf(tpccProcess.getWarehouseId()), 3),
				StringUtils.leftPad(String.valueOf(tpccProcess.getRetryCount()), 3)
		);
		
		// group transactions into 100 ms intervals
		long timeKey = latency / 100;
		timeKey *= 100;

		Class<? extends TpccTestProcess> processType = tpccProcess.getClass();
		
		processingTimeByTxType.putIfAbsent(timeKey, new HashMap<>());
		Map<Class<? extends TpccTestProcess>, Integer> typeMap = processingTimeByTxType.get(timeKey);
		
		typeMap.putIfAbsent(processType, 0);
		
		int value = typeMap.get(processType);
		
		// replace the existing value with the incremented one
		typeMap.put(processType, ++value);
		
		if(tpccProcess.isTerminalBased()) {
			// return terminal to the pool at this time
			long terminalFreeupTime = System.currentTimeMillis() + tpccProcess.getThinkTimeInMs();
			
			// very unlikely, but not impossible
			while(terminalFreeupTimes.containsKey(terminalFreeupTime)) {
				// delay by 1ms more
				terminalFreeupTime++;
			}
			
			terminalFreeupTimes.put(terminalFreeupTime, tpccProcess.getWarehouseId());
		}
		
		assert assertSlots();
	}
}
