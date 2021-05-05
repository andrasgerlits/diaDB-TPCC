package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Map;

import com.dianemodb.ConversationId;
import com.dianemodb.exception.DiaDbException;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.integration.test.BaseProcess;
import com.dianemodb.integration.test.BaseProcess.Result;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.metaschema.DianemoApplication;

import fj.data.Either;

public class TpccProcessManager extends ProcessManager {

	private final TpccProcessMaker processMaker;
	private TpccProcessScheduler scheduler;
	
	public TpccProcessManager(
			DianemoApplication application, 
			List<ServerComputerId> leafComputers,
			int concurrentRequestNumber
	) {
		super(leafComputers, concurrentRequestNumber, application);
		
		this.processMaker = new TpccProcessMaker(application);
		this.scheduler = 
				new TerminalBasedProcessScheduler(
						(w, d) -> processMaker.createNextProcess(w, d, 0),
						batch -> super.sendNextSteps(batch)
				);
	}
	
	public Result failed(ConversationId conversationId, Throwable ex, BaseProcess testProcess) {
		return scheduler.failed(conversationId, ex, testProcess);
	}

	@Override
	public synchronized boolean isFinished() {
		// Doesn't finish until cancelled
		return false;
	}
	
	@Override
	public void process(
			Map<ConversationId, Either<Object, ? extends DiaDbException>> results
	) {
		super.process(results);
		scheduler.resume();
	}


	@Override
	protected void success(BaseProcess process) {
		scheduler.finished((TpccTestProcess) process );
	}
}
