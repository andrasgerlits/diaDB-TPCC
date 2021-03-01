package com.dianemodb.tpcc.transaction;

import com.dianemodb.ConversationId;
import com.dianemodb.integration.test.BaseProcess;
import com.dianemodb.integration.test.BaseProcess.Result;

public interface TpccProcessScheduler {

	/**
	 * Called when the process has nothing more to do, either because it finished or because
	 * it failed. This is think-time agnostic, so is called immediately once this decision has
	 * been made.
	 * */
	public void finished(TpccTestProcess tpccProcess);

	public Result failed(ConversationId conversationId, Throwable ex, BaseProcess testProcess);

	public boolean resume();

}
