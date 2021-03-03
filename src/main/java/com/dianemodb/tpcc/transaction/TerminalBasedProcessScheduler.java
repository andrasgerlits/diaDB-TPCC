package com.dianemodb.tpcc.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ConversationId;
import com.dianemodb.integration.test.BaseProcess;
import com.dianemodb.integration.test.BaseProcess.Result;
import com.dianemodb.integration.test.NextStep;
import com.dianemodb.tpcc.Constants;

public class TerminalBasedProcessScheduler implements TpccProcessScheduler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TerminalBasedProcessScheduler.class.getName());

	/**
	 * 
	 * */
	private final TerminalPool terminals;
	
	/**
	 * Processes, which have already failed and are waiting to be 
	 * retried. Each is blocking a terminal.
	 * */
	private final List<NextStep> toRetry = new LinkedList<>();
	
	private long lastDumpTime = System.currentTimeMillis();

	/**
	 * Processes blocking a terminal and waiting for their time, so that
	 * their request is sent to the server for processing. 
	 * */
	private final List<NextStep> processesInKeyingTime = new LinkedList<>();	

	private int retryCount = 0;
	
	private final Random random = new Random();

	private static final int TIME_MEASUREMENT_INTERVAL = 1 * 60 * 1000;
	
	/**
	 * In a single configured window, how many types of transactions were executed in 
	 * */
	private final Map<Long, Map<Class<? extends TpccTestProcess>, Integer>> processingTimeByTxType = 
			new HashMap<>();
	
	private final BiFunction<Short, Byte, TpccTestProcess> processFactory;
	
	private final Consumer<List<NextStep>> sender;

	public TerminalBasedProcessScheduler(
			BiFunction<Short, Byte, TpccTestProcess> processFactory,
			Consumer<List<NextStep>> sender			
	) {
		this.terminals = new TerminalPool();
		this.processFactory = processFactory;
		this.sender = sender;
		
		/*
		 * we can start a tx for each terminal right away, 
		 * start with an initial variance of 10 seconds to space out the first requests 
		 */
		resume();
	}
	
	@Override
	public Result failed(ConversationId conversationId, Throwable ex, BaseProcess testProcess) {
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Process failed {}", testProcess);
		}

		TpccTestProcess process = (TpccTestProcess) testProcess; 
		
		// if this was an expected diadb exception, like concurrent commit, so safe to retry
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
		return BaseProcess.FINISHED;
	}

	public boolean resume() {
		int numberOfNewProcessesToStart = terminals.availableTerminals() + toRetry.size();

		if(LOGGER.isDebugEnabled()) {
	 		LOGGER.debug(
					"available terminals {} To start {}, retry {} keying {} terminals {}",
					terminals.availableTerminals(),
					numberOfNewProcessesToStart, 
					toRetry.size(), 
					processesInKeyingTime.size(), 
					terminals.availableTerminals()
			);
			
			if(!processesInKeyingTime.isEmpty()) {
				Collections.sort(processesInKeyingTime, TpccTestProcess.REQUEST_START_TIME_COMPARATOR);
				
				long minRequestTime = 
						((TpccTestProcess) processesInKeyingTime.get(0).getProcess())
							.getMinInitialRequestTime();
				
				
				LOGGER.debug("Earliest starts in {} ms", minRequestTime - System.currentTimeMillis());
			}
		}

		
		// if nothing to start
		if(numberOfNewProcessesToStart > 0) {
			// retried processes will have 0 keying time, so will be included in the started processes
			List<NextStep> steps = nextProcess(numberOfNewProcessesToStart);
			
			/*
			 * the ones being retried will be removed from here later in the method, as their
			 * minimum request time must already be in the past. 
			 */
			processesInKeyingTime.addAll(steps);
		}
		
		if(processesInKeyingTime.isEmpty()) {
			return true;
		}
			
		long now = System.currentTimeMillis();
		
		// list also contains processes being retried
		List<NextStep> stepsToAbort = 
				processesInKeyingTime.stream()
					.filter(n -> ((TpccTestProcess)n.getProcess()).isLate())
					.collect(Collectors.toList());
		
		if(!stepsToAbort.isEmpty()) {
			processesInKeyingTime.removeAll(stepsToAbort);
			
			LOGGER.warn("Aborting before request even sent {}", stepsToAbort);
			
			stepsToAbort.forEach(s -> finished( (TpccTestProcess) s.getProcess()) );
		}
		
		List<NextStep> processesToStart = 
				processesInKeyingTime.stream()
					.filter(s -> ((TpccTestProcess) s.getProcess()).getMinInitialRequestTime() <= now )
					.collect(Collectors.toList());
		
		// the ones with the lower start-time should be sent first, FIFO
		Collections.sort(processesToStart, TpccTestProcess.REQUEST_START_TIME_COMPARATOR);
		
		processesInKeyingTime.removeAll(processesToStart);
		
		sender.accept(processesToStart);
		
		if(LOGGER.isDebugEnabled() && processesToStart.size() > 0) {
			LOGGER.debug("Started {} processes", processesToStart.size());
		}
		
		logTimes();
		return true;
	}
	
	private void logTimes() {
		long now;
		if((now = System.currentTimeMillis()) - lastDumpTime < TIME_MEASUREMENT_INTERVAL) {
			LOGGER.trace("until next log {}", TIME_MEASUREMENT_INTERVAL - (now - lastDumpTime));
			return;
		}
		
		Map<Class<? extends TpccTestProcess>, Integer> failures = new HashMap<>();
		Map<Class<? extends TpccTestProcess>, Pair<Integer, Long>> successes = new HashMap<>();
		
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
					successes.putIfAbsent(c, Pair.of(0,0L));
					Pair<Integer, Long> current = successes.get(c);

					long totalTime = highest * number;
					successes.put(c, Pair.of(current.getKey() + number, current.getValue() + totalTime));
				}
			}
			
			timeslots.remove(highest);
		}
		
		for(Entry<Class<? extends TpccTestProcess>, Integer> e : failures.entrySet()) {
			LOGGER.info("Failure {} {}", e.getKey().getSimpleName(), e.getValue());
		}
		
		for(Entry<Class<? extends TpccTestProcess>, Pair<Integer, Long>> e : successes.entrySet()) {
			Pair<Integer, Long> pair = e.getValue();
			int avgTime = (int) (pair.getValue() / pair.getKey());
			
			LOGGER.info(
				"Success {} {} avg {} ms", 
				e.getKey().getSimpleName(),
				e.getValue().getKey(), 
				avgTime 
			);
		}
		
		LOGGER.info("Retries {}", retryCount);
		LOGGER.info("Keying {}", processesInKeyingTime.size());
		retryCount = 0;
		
		terminals.logState();
		
		lastDumpTime = now;
		processingTimeByTxType.clear();
	}
	
	protected List<NextStep> nextProcess(int number) {
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
		byte districtId = (byte) random.nextInt(Constants.DISTRICT_PER_WAREHOUSE);

		TpccTestProcess process = processFactory.apply(warehouseId, districtId);

		if(process.isTerminalBased()) {
			terminals.borrow(process);
		}
		
		NextStep nextStep = process.start();
		
		return nextStep;
	}

	@Override
	public void finished(TpccTestProcess tpccProcess) {
		tpccProcess.finished();
		
		if(LOGGER.isDebugEnabled()) {
			if(tpccProcess.isLate()) {
				LOGGER.debug(
						"late {} {} {} {} {}", 
						StringUtils.leftPad(tpccProcess.getClass().getSimpleName(), 15),
						StringUtils.leftPad(String.valueOf(tpccProcess.getTpccLatency()), 10),
						StringUtils.leftPad(String.valueOf(tpccProcess.txLatency()), 10),						
						StringUtils.leftPad(String.valueOf(tpccProcess.getWarehouseId()), 3),
						StringUtils.leftPad(String.valueOf(tpccProcess.getRetryCount()), 3)
				);
			}
			else {
				LOGGER.debug(
						"okay {} {} {} {} {}", 
						StringUtils.leftPad(tpccProcess.getClass().getSimpleName(), 15),
						StringUtils.leftPad(String.valueOf(tpccProcess.getTpccLatency()), 10),
						StringUtils.leftPad(String.valueOf(tpccProcess.txLatency()), 10),						
						StringUtils.leftPad(String.valueOf(tpccProcess.getWarehouseId()), 3),
						StringUtils.leftPad(String.valueOf(tpccProcess.getRetryCount()), 3)
				);
			}
		}
		
		// group transactions into 100 ms intervals
		long timeKey = tpccProcess.getTpccLatency() / 100;
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


}
