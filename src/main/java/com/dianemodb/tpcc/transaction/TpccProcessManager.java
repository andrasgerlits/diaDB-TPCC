package com.dianemodb.tpcc.transaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

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
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessManager.class.getName());
	
	@FunctionalInterface
	private interface TpccProcessFactory {
		TpccTestProcess get(short warehouseId, byte districtId);
	}
	
	private final RangeMap<Integer, TpccProcessFactory> factoryByWeight = TreeRangeMap.create();
	
	/**
	 * The pool of terminals, from which the 
	 * - first one is taken each time a new TX started 
	 * - the last one is appended each time a TX finished 
	 */
	private final List<Short> freeTerminals = new LinkedList<>();
	
	private Map<Long, Map<Class<? extends TpccTestProcess>, Long>> timeTookByTxType = 
			new HashMap<>();

	private final int sum;
	
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
	
	private List<NextStep> toRetry = new LinkedList<>();
	
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

	/**
	 * The number of processes which could be started (because we have more remaining and
	 * because we didn't hit the upper limit on the number of parallel TX), but which 
	 * couldn't for some reason known only to the specific implementation.
	 * */
	private int outstanding = 0;

	public void process(Map<ConversationId, Either<Object, ? extends Throwable>> results) {
		super.process(results);
		
		int numberToStart = freeTerminals.size() + outstanding;
		int started = startNewProcess(numberToStart);
		outstanding = numberToStart - started;
		
		if(LOGGER.isDebugEnabled() && (started > 0 || outstanding > 0)) {
			LOGGER.debug("Started {} processes, outstanding {}", started, outstanding);
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

	private NextStep startNewProcess() {
		// choose a random number from the grand total
		int processId = getRandom().nextInt(sum);
		
		byte districtId = (byte) getRandom().nextInt(Constants.DISTRICT_PER_WAREHOUSE);
		short warehouseId = freeTerminals.remove(0);
		
		
		TpccProcessFactory factory = factoryByWeight.get(processId);
		TpccTestProcess process = factory.get(warehouseId, districtId);
		
		NextStep nextStep = process.start();
		
		if(LOGGER.isInfoEnabled()) {
			// add as many warehouse-ids as there are terminals
			long now = System.currentTimeMillis();
			LOGGER.info(
					"Started process warehouse {} wait {} ms {}", 
					warehouseId, 
					process.getInitialRequestStartTime()- now, 
					process
			);
		}

		return nextStep;
	}

	private void returnNoLongerUsedTerminals() {
		// clean up the free terminals list, return the ones which are no longer used
		Collection<Short> terminals = 
				terminalFreeupTimes.subMap(0L, System.currentTimeMillis())
					.values();

		if(LOGGER.isInfoEnabled() && !terminals.isEmpty()) {
			// add as many warehouse-ids as there are terminals
			LOGGER.info("Freeing up terminals {}", terminals);
		}

		freeTerminals.addAll(terminals);
	}
	
	private NavigableMap<Long, Short> terminalFreeupTimes = new TreeMap<>();
	
	@Override
	protected void success(TestProcess process) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Process success {}", process);
		}
		
		TpccTestProcess tpccProcess = (TpccTestProcess) process;
		long timeTook = System.currentTimeMillis() - tpccProcess.getStartTime();
		
		// group transactions into 100 ms intervals
		long timeKey = timeTook / 100;

		Class<? extends TpccTestProcess> processType = tpccProcess.getClass();
		
		Map<Class<? extends TpccTestProcess>, Long> typeMap = 
				timeTookByTxType.computeIfAbsent(
					timeKey, 
					k -> {
						Map<Class<? extends TpccTestProcess>, Long> map = new HashMap<>();
						// init to zero
						map.put(processType, Long.valueOf(0));
						
						return map;
					}
				);
		
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
