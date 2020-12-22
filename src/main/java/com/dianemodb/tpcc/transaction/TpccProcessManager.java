package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.metaschema.SQLServerApplication;

import fj.data.Either;

public class TpccProcessManager extends ProcessManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(TpccProcessManager.class.getName());

	private final TpccProcessMaker processMaker;
	private TpccProcessScheduler scheduler;
	
	public TpccProcessManager(
			SQLServerApplication application, 
			List<ServerComputerId> leafComputers,
			int concurrentRequestNumber
	) {
		super(leafComputers, concurrentRequestNumber);
		
		this.processMaker = new TpccProcessMaker(application);
		this.scheduler = 
				new TerminalBasedProcessScheduler(
						(w, d) -> processMaker.createNextProcess(w, d, 0),
						batch -> super.sendNextSteps(batch)
				);

/*
		this.scheduler = 
				new TpccStartupScheduler(
						0,
						(w, d) -> processMaker.createNextProcess(w, d, 0),
						batch -> super.sendNextSteps(batch)
				);
*/
	}
	
	public Result failed(ConversationId conversationId, Throwable ex, TestProcess testProcess) {
		return scheduler.failed(conversationId, ex, testProcess);
	}

	@Override
	public synchronized boolean isFinished() {
		// Tick-tock, the party don't stop
		return false;
	}

	private void finished(TpccTestProcess tpccProcess) {
		scheduler.finished(tpccProcess);
	}
	
	public void process(Map<ConversationId, Either<Object, ? extends Throwable>> results) {
		super.process(results);
		
		if(!scheduler.resume()) {
			assert scheduler instanceof TpccStartupScheduler;

			scheduler = 			
					new TerminalBasedProcessScheduler(
							(w, d) -> processMaker.createNextProcess(w, d, 0),
							batch -> super.sendNextSteps(batch)
					);
		}
	}
	
	@Override
	protected void success(TestProcess process) {
		finished((TpccTestProcess) process);
	}
}
