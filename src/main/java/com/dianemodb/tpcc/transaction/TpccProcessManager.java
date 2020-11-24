package com.dianemodb.tpcc.transaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.exception.ClientInitiatedRollbackTransactionException;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.NextStep;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.Constants;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import fj.data.Either;

public class TpccProcessManager extends ProcessManager {
	
	@FunctionalInterface
	private interface TpccProcessFactory {
		TpccTestProcess get(short warehouseId, byte districtId);
	}
	
	private static final int TIME_MEASUREMENT_INTERVAL = 5 * 60 * 1000;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessManager.class.getName());
	
	private final RangeMap<Integer, TpccProcessFactory> factoryByWeight = TreeRangeMap.create();
	
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
	private Map<Long, Map<Class<? extends TpccTestProcess>, Long>> processingTimeByTxType = 
			new HashMap<>();
	
	private final int sum;
	
	private NavigableMap<Long, Short> terminalFreeupTimes = new TreeMap<>();
	
	private List<NextStep> toRetry = new LinkedList<>();

	public TpccProcessManager(
			SQLServerApplication application, 
			List<ServerComputerId> leafComputers
	) {
		super(application, leafComputers);
		
		List<Pair<Integer, TpccProcessFactory>> factories = 
				List.of(
						Pair.of(10, (w, d) -> new NewOrder(random, randomComputer(), application, w, d)),
						Pair.of(10, (w, d) -> new Payment(random, randomComputer(), application, w, d)),
						Pair.of(1, (w, d) -> new OrderStatus(random, randomComputer(), application, w, d)),
						Pair.of(1, (w, d) -> new Delivery(random, randomComputer(), application, w, d)),
						Pair.of(1, (w, d) -> new StockLevel(random, randomComputer(), application, w, d))
				);
		
		int totalSoFar = 0;
		for(Pair<Integer, TpccProcessFactory> p : factories) {
			int previousTotal = totalSoFar;
			totalSoFar += p.getKey();
			
			factoryByWeight.put(Range.closedOpen(previousTotal, totalSoFar), p.getValue());
		}
		
		this.sum = totalSoFar;
		
		for( short i = 0; i < Constants.NUMBER_OF_WAREHOUSES; i++ ) {
			for(short j = 0; j < Constants.TERMINAL_PER_WAREHOUSE; j++) {
				freeTerminals.add(i);
			}
		}
		
		// we can start a tx for each terminal right away
		startNewProcess(freeTerminals.size());
	}

	@Override
	protected void failed(ConversationId conversationId, Throwable ex, TestProcess testProcess) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Process failed {}", testProcess);
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
			toRetry.add(process.start());
		}	
	}

	@Override
	protected List<NextStep> nextProcess(int number) {
		List<NextStep> result = new LinkedList<>();
		
		// if there was anything to retry, return that first
		while(!toRetry.isEmpty()) {
			result.add(toRetry.remove(0));

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
			result.add(startNewProcess());
		}
		
		return result;
	}

	public void process(Map<ConversationId, Either<Object, ? extends Throwable>> results) {
		super.process(results);
		
		int numberToStart = freeTerminals.size() + outstanding;
		int started = startNewProcess(numberToStart);
		outstanding = numberToStart - started;
		
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
			Map<Class<? extends TpccTestProcess>, Long> m = processingTimeByTxType.get(highest);
			
			for(Map.Entry<Class<? extends TpccTestProcess>, Long> e : m.entrySet()) {
				Class<? extends TpccTestProcess> c = e.getKey();
				long whenLate = TpccTestProcess.MAX_TIMES_BY_CLASS.get(c);
				
				// report as late
				if(highest >= whenLate) {
					failures.putIfAbsent(c, 0);
					failures.put(c, failures.get(c) + 1);
				}
				else {
					successes.putIfAbsent(c, 0);
					successes.put(c, successes.get(c) + 1);				
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
		
		lastDumpTime = now;
		processingTimeByTxType.clear();
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
	}
	
	private NextStep startNewProcess() {
		// choose a random number from the grand total
		int processId = getRandom().nextInt(sum);
		
		byte districtId = (byte) getRandom().nextInt(Constants.DISTRICT_PER_WAREHOUSE);
		short warehouseId = freeTerminals.remove(0);
		
		
		TpccProcessFactory factory = factoryByWeight.get(processId);
		TpccTestProcess process = factory.get(warehouseId, districtId);
		
		NextStep nextStep = process.start();
		
		if(LOGGER.isDebugEnabled()) {
			// add as many warehouse-ids as there are terminals
			long now = System.currentTimeMillis();
			LOGGER.debug(
					"Started process warehouse {} wait {} ms {}", 
					warehouseId, 
					process.getInitialRequestStartTime()- now, 
					process
			);
		}

		return nextStep;
	}
	
	@Override
	protected void success(TestProcess process) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Process success {}", process);
		}
		
		TpccTestProcess tpccProcess = (TpccTestProcess) process;
		long timeTook = System.currentTimeMillis() - tpccProcess.getInitialRequestStartTime();
		
		// group transactions into 100 ms intervals
		long timeKey = timeTook / 100;
		timeKey *= 100;

		Class<? extends TpccTestProcess> processType = tpccProcess.getClass();
		
		Map<Class<? extends TpccTestProcess>, Long> typeMap = 
				processingTimeByTxType.computeIfAbsent(
					timeKey, 
					k -> {
						Map<Class<? extends TpccTestProcess>, Long> map = new HashMap<>();
						// init to zero
						map.put(processType, Long.valueOf(0));
						
						return map;
					}
				);
		
		typeMap.putIfAbsent(processType, 0L);
		
		// replace the existing value with the incremented one
		typeMap.put(processType, typeMap.get(processType) + 1);
		
		// return terminal to the pool at this time
		long terminalFreeupTime = System.currentTimeMillis() + tpccProcess.getThinkTimeInMs();
		
		// very unlikely, but not impossible
		while(terminalFreeupTimes.containsKey(terminalFreeupTime)) {
			// delay by 1ms more
			terminalFreeupTime++;
		}
		
		terminalFreeupTimes.put(terminalFreeupTime, tpccProcess.getWarehouseId());
	}
}
