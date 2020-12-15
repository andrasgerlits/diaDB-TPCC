package com.dianemodb.tpcc.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dianemodb.ConversationId;
import com.dianemodb.integration.test.NextStep;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.tpcc.Constants;

public class TpccStartupScheduler implements TpccProcessScheduler {
	
	private static final int STARTUP_BATCH_NUMBER = 100;
	private final Consumer<List<NextStep>> sender;
	private final List<Supplier<TpccTestProcess>> processes = new LinkedList<>();
	private int finished = 0;
	private int finishedNow = 0;
	
	private static final int TOTAL_NUMBER = 
			Constants.NUMBER_OF_WAREHOUSES * Constants.DISTRICT_PER_WAREHOUSE * STARTUP_BATCH_NUMBER; 
	
	public TpccStartupScheduler(
			BiFunction<Short, Byte, TpccTestProcess> processFactory,
			Consumer<List<NextStep>> sender			
	) {
		this.sender = sender;
		
		for(int i = 0; i < STARTUP_BATCH_NUMBER; i++) {
			for(short w=0; w < Constants.NUMBER_OF_WAREHOUSES; w++) {
				final short wId = w;
				for(byte d = 0; d < Constants.DISTRICT_PER_WAREHOUSE; d++) {
					final byte dId = d;
					processes.add(() -> processFactory.apply(wId, dId));
				}		
			}
		}
		
		Collections.shuffle(processes);
		
		sendBatch(1000);
	}

	@Override
	public void finished(TpccTestProcess tpccProcess) {
		finishedNow++;
	}

	@Override
	public Result failed(ConversationId conversationId, Throwable ex, TestProcess testProcess) {
		finished((TpccTestProcess) testProcess);
		return TpccTestProcess.FINISHED;
	}

	@Override
	public boolean resume() {
		sendBatch(finishedNow);
		finished += finishedNow;
		finishedNow = 0;
		return finished < TOTAL_NUMBER;
	}

	private void sendBatch(int number) {
		final int batchSize = Math.min(number, processes.size());
		
		List<NextStep> batch = new ArrayList<>(batchSize);
		while(batch.size() < batchSize) {
			NextStep process = processes.remove(0).get().start();
			batch.add(process);
		}
		sender.accept(batch);
	}
}

