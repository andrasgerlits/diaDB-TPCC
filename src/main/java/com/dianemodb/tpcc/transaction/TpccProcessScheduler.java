package com.dianemodb.tpcc.transaction;

import com.dianemodb.ConversationId;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;

public interface TpccProcessScheduler {

	public void finished(TpccTestProcess tpccProcess);

	public Result failed(ConversationId conversationId, Throwable ex, TestProcess testProcess);

	public boolean resume();

}
