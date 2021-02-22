package com.dianemodb.tpcc.transaction;

import java.util.List;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.metaschema.SQLServerApplication;

public class TpccProcessManager extends ProcessManager {

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
	}
	
	public Result failed(ConversationId conversationId, Throwable ex, TestProcess testProcess) {
		return scheduler.failed(conversationId, ex, testProcess);
	}

	@Override
	public synchronized boolean isFinished() {
		// Doesn't finish until cancelled
		return false;
	}

	@Override
	protected void success(TestProcess process) {
		scheduler.finished((TpccTestProcess) process );
	}
}
