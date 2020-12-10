package com.dianemodb.tpcc.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
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
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.integration.test.NextStep;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.schema.WarehouseTable;

import fj.data.Either;

public class TpccProcessManager extends ProcessManager {
	
	@FunctionalInterface
	private interface TpccProcessFactory {
		TpccTestProcess get(short warehouseId, byte districtId, String uuid);
	}
	
	private static final int TIME_MEASUREMENT_INTERVAL = 1 * 60 * 1000;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessManager.class.getName());

	private long lastDumpTime = System.currentTimeMillis();
	
	private final TerminalPool terminals;

	/**
	 * In a single configured window, how many types of transactions were executed in 
	 * */
	private final Map<Long, Map<Class<? extends TpccTestProcess>, Integer>> processingTimeByTxType = 
			new HashMap<>();
	
	private final Map<Short, List<TpccProcessFactory>> factoriesByWarehouse = new HashMap<>();
	
	private final List<NextStep> toRetry = new LinkedList<>();
	
	private final List<TpccProcessFactory> prototypeList;

	private final List<NextStep> processesInKeyingTime = new LinkedList<>();
	
	private int retryCount = 0;
	
	public TpccProcessManager(
			SQLServerApplication application, 
			List<ServerComputerId> leafComputers,
			int concurrentRequestNumber
	) {
		super(leafComputers, concurrentRequestNumber);
		
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
		
		terminals = new TerminalPool(false);
		
		for(short i = 0; i < Constants.NUMBER_OF_WAREHOUSES; i++ ) {
			List<TpccProcessFactory> factoryList = new LinkedList<>(prototypeList);
			Collections.shuffle(factoryList);
			factoriesByWarehouse.put(i, factoryList);
		}
		
		/*
		 * we can start a tx for each terminal right away, 
		 * start with an initial variance of 10 seconds to space out the first requests 
		 */
		List<NextStep> steps = nextProcess(terminals.availableTerminals());
		super.sendNextSteps(steps);
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
		}

		TpccTestProcess process = (TpccTestProcess) testProcess; 
		
		// if this failure was triggered by the client-API (so a designed failure)
		if(ex instanceof ClientInitiatedRollbackTransactionException) {
			// these are supposed to be finished transactions
			success(testProcess);
		}
		// if this was an expected diadb exception, like concurrent commit, so safe to retry
		else {
			// if process is already late, it has failed, stop retrying
			if(!process.isLate()) {
				retryCount++;
				toRetry.add(process.cancelAndRetry());
				
				if(process.isTerminalBased()) {
					terminals.ping(process);
				}
			}
			else {
				// handle it like any other timed out process
				finished((TpccTestProcess) process);
			}
		}
		return TestProcess.FINISHED;
	}

	private List<NextStep> nextProcess(int number) {
		List<NextStep> result = new LinkedList<>();
		
		// if there was anything to retry, return that first
		while(!toRetry.isEmpty()) {
			NextStep nextStep = toRetry.remove(0);
			result.add(nextStep);
			
			if(result.size() == number) {
				return result;
			}
		}
		
		while(
			!terminals.isEmpty() 
			&& result.size() < number 
		) {
			result.add(startNewProcess());
		}
		
		return result;
	}
	
	private NextStep startNewProcess() {
		Optional<Short> maybeWarehouseId = terminals.randomWarehouseWithFreeTerminal();
		
		// should have checked if there were available values
		assert maybeWarehouseId.isPresent();
		
		short warehouseId = maybeWarehouseId.orElseThrow(); 
		byte districtId = (byte) getRandom().nextInt(Constants.DISTRICT_PER_WAREHOUSE);

		TpccTestProcess process = createNextProcess(districtId, warehouseId, 0);

		if(process.isTerminalBased()) {
			terminals.borrow(process);
		}
		
		NextStep nextStep = process.start();
		
		return nextStep;
	}

	private TpccTestProcess createNextProcess(
			byte districtId, 
			short warehouseId, 
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
	
	public void process(Map<ConversationId, Either<Object, ? extends Throwable>> results) {
		super.process(results);
		
		int numberToStart = terminals.availableTerminals() + toRetry.size() - processesInKeyingTime.size();
		
		LOGGER.debug(
				"To start {}, retry {} keying {} terminals {}", 
				numberToStart, 
				toRetry.size(), 
				processesInKeyingTime.size(), 
				terminals.availableTerminals()
		);
		
		// if nothing to start
		if(numberToStart == 0) {
			return;
		}
		
		// retried processes will have 0 keying time, so will be included in the started processes
		List<NextStep> steps = nextProcess(numberToStart);
		processesInKeyingTime.addAll(steps);
		
		long now = System.currentTimeMillis();
		
		List<NextStep> stepsToAbort = 
				steps.stream()
				.filter(n -> ((TpccTestProcess)n.getProcess()).isLate())
				.collect(Collectors.toList());
		
		if(!stepsToAbort.isEmpty()) {
			processesInKeyingTime.removeAll(stepsToAbort);
			
			LOGGER.warn("Aborting before request even sent {}", stepsToAbort);
			
			stepsToAbort.forEach(s -> finished( (TpccTestProcess) s.getProcess()) );
		}
		
		List<NextStep> processesToStart = 
				processesInKeyingTime.stream()
					.filter(s -> ((TpccTestProcess) s.getProcess()).getInitialRequestTime() >= now )
					.collect(Collectors.toList());
		
		Collections.sort(processesToStart, TpccTestProcess.REQUEST_START_TIME_COMPARATOR);
		
		processesInKeyingTime.removeAll(processesToStart);
		
		super.sendNextSteps(processesToStart);
		
		if(LOGGER.isDebugEnabled() && processesToStart.size() > 0) {
			LOGGER.debug("Started {} processes", processesToStart);
		}
		
		logTimes();
	}

	private void logTimes() {
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
		
		LOGGER.info("Retries {}", retryCount);
		LOGGER.info("Keying {}", processesInKeyingTime.size());
		retryCount = 0;
		
		terminals.logState();
		LOGGER.info("{}", super.aggregator.toDetailsString());
		
		lastDumpTime = now;
		processingTimeByTxType.clear();
	}
	
	private void finished(TpccTestProcess tpccProcess) {
		long latency = System.currentTimeMillis() - tpccProcess.getInitialRequestTime();

		if(LOGGER.isDebugEnabled()) {
			if(tpccProcess.isLate()) {
				LOGGER.debug(
						"Process late {} {} {} {}", 
						StringUtils.leftPad(tpccProcess.getClass().getSimpleName(), 15),
						StringUtils.leftPad(String.valueOf(latency), 10),
						StringUtils.leftPad(String.valueOf(tpccProcess.getWarehouseId()), 3),
						StringUtils.leftPad(String.valueOf(tpccProcess.getRetryCount()), 3),
						tpccProcess.getUiid()
				);
			}
			else {
				LOGGER.debug(
						"Process finished {} {} {} {}", 
						StringUtils.leftPad(tpccProcess.getClass().getSimpleName(), 15),
						StringUtils.leftPad(String.valueOf(latency), 10),
						StringUtils.leftPad(String.valueOf(tpccProcess.getWarehouseId()), 3),
						StringUtils.leftPad(String.valueOf(tpccProcess.getRetryCount()), 3),
						tpccProcess.getUiid()
				);
			}
		}
		
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
			terminals.processFinished(tpccProcess);
		}
	}
	
	@Override
	protected void success(TestProcess process) {
		finished((TpccTestProcess) process);
	}
}
