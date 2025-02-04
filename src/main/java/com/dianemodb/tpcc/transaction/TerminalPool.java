package com.dianemodb.tpcc.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.tpcc.Constants;

public class TerminalPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(TerminalPool.class.getName());
	
	private static final int BORROW_TIMEOUT = 30 * 1000 * 60;

	/**
	 * The pool of terminals, from which the 
	 * - first one is taken each time a new TX started 
	 * - the last one is appended each time a TX finished
	 * 
	 *  We're using AtomicInteger to be mutable, not for syncing 
	 */
	private final Map<Short, AtomicInteger> freeTerminals = new HashMap<>();
	
	private final NavigableMap<Long, Short> terminalFreeupTimes = new TreeMap<>();
	
	private final Map<TpccTestProcess, Long> borrowTime = new LinkedHashMap<>();
	
	public TerminalPool() {
		for( short i = 0; i < Constants.NUMBER_OF_WAREHOUSES; i++ ) {
			freeTerminals.put(i, new AtomicInteger(Constants.TERMINAL_PER_WAREHOUSE));
		}
	}
	
	public void logState() {
		LOGGER.info("free {}", availableTerminals());
		LOGGER.info("think time {}", terminalFreeupTimes.size());
		LOGGER.info("borrowed {}", borrowed());
	}

	private int borrowed() {
		return borrowTime.size();
	}

	public int availableTerminals() {
		recalculatePoolSize();
		checkTimeouts();

		return freeTerminals.values()
				.stream()
				.mapToInt(e -> e.intValue())
				.sum();
	}

	public boolean isEmpty() {
		Optional<Entry<Short, AtomicInteger>> maybeFreeTerminal = maybeAnyFreeTerminal();
		return !maybeFreeTerminal.isPresent();
	}
	
	public Optional<Short> randomWarehouseWithFreeTerminal() {
		return maybeAnyFreeTerminal().map(Entry::getKey);
	}

	private Optional<Entry<Short, AtomicInteger>> maybeAnyFreeTerminal() {
		recalculatePoolSize();

		// returns the terminal from a warehouse with the most free terminals
		return freeTerminals.entrySet()
					.stream()
					.filter( e -> e.getValue().intValue() > 0 )
					// descending order
					.sorted( (e1, e2) -> Integer.valueOf(e2.getValue().intValue()).compareTo(e1.getValue().intValue()) )
					.findFirst();
	}
	
	public void borrow(TpccTestProcess process) {
		
		recalculatePoolSize();
		
		// shouldn't have been called if empty
		assert !isEmpty();
		
		long now = System.currentTimeMillis();
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Borrowing terminal {}", process.getMinInitialRequestTime() - now);
		}
		
		short warehouseId = process.getWarehouseId();
		
		int freeTerminalsForWarehouse = freeTerminals.get(warehouseId).decrementAndGet();
		
		borrowTime.put(process, now);
		
		assert freeTerminalsForWarehouse >= 0;
	}

	public void processFinished(TpccTestProcess tpccProcess) {
		// even if it timed out, we graciously forgive
		borrowTime.remove(tpccProcess);
		
		checkTimeouts();
		recalculatePoolSize();
		
		// return terminal to the pool at this time
		long terminalFreeupTime = System.currentTimeMillis() + tpccProcess.getThinkTimeInMs();
		
		// very unlikely, but not impossible
		while(terminalFreeupTimes.containsKey(terminalFreeupTime)) {
			// delay by 1ms more
			terminalFreeupTime++;
		}
		
		terminalFreeupTimes.put(terminalFreeupTime, tpccProcess.getWarehouseId());
	}
	
	public void ping(TpccTestProcess tpccProcess) {
		borrowTime.remove(tpccProcess);
		borrowTime.put(tpccProcess, System.currentTimeMillis());
	}
	
	private void checkTimeouts() {
		if(borrowTime.isEmpty()) {
			return;
		}
		
		// if anything is older than this, throw error
		long maxAllowedTime = System.currentTimeMillis() - BORROW_TIMEOUT;

		List<Pair<Long, TpccTestProcess>> offendingProcesses = new LinkedList<>();
		
		Iterator<Entry<TpccTestProcess, Long>> entryIter = borrowTime.entrySet().iterator();
		while(entryIter.hasNext()) {
			Entry<TpccTestProcess, Long> p = entryIter.next();
			
			if(p.getValue() <= maxAllowedTime) {
				offendingProcesses.add(
					Pair.of(
						System.currentTimeMillis() - p.getValue(),
						p.getKey()
					)
				);
			}
			else {
				/*
				 * the elements are incremental in time, so the lowest one is the oldest,
				 * so no point in checking beyond the first one that's still legal
				 */
				break;
			}
		}
		
		if(!offendingProcesses.isEmpty()) {
			LOGGER.error("Offending processes {}", offendingProcesses);			
			throw new IllegalStateException(offendingProcesses.toString());
		}
	}

	/**
	 * Clean up potentially freed up terminals. The time for these is marked
	 * when the process has finished, but they're only marked as available just
	 * before they're needed.
	 */
	private synchronized void recalculatePoolSize() {
		if(terminalFreeupTimes.isEmpty()) {
			return;
		}
		
		// clean up the free terminals list, return the ones which are no longer used
		Map<Long, Short> freeUpNow = 
				new HashMap<>(terminalFreeupTimes.subMap(0L, System.currentTimeMillis()));

		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Returning {}", freeUpNow.size());
		}
		
		for(Short warehouseId : freeUpNow.values()) {
			int currentlyFree = freeTerminals.get(warehouseId).incrementAndGet();
			
			// can't be 0, one was just returned
			assert currentlyFree > 0 && currentlyFree <= Constants.TERMINAL_PER_WAREHOUSE; 
		}
		
		for(Long key : freeUpNow.keySet()) {
			terminalFreeupTimes.remove(key);
		}		
	}
}
