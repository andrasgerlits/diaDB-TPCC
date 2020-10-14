package com.dianemodb.tpcc.transaction;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.UserRecord;
import com.dianemodb.event.ExecuteWorkflowEvent;
import com.dianemodb.event.tx.CommitTransactionEvent;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.id.TransactionId;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.version.ReadVersion;
import com.dianemodb.workflow.query.QueryWorkflow;
import com.dianemodb.workflow.query.QueryWorkflowInput;
import com.dianemodb.workflow.write.ChangeRecordsWorkflowInput;
import com.dianemodb.workflow.write.ModifyRecordsWorkflow;

public abstract class TpccTestProcess extends TestProcess {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccTestProcess.class.getName());	

	protected static boolean hasString(String value, String searched) {
		return value != null && value.contains(searched);
	}
	
	@SuppressWarnings("unchecked")
	public static <R extends UserRecord> RecordWithVersion<R> singleFromResultList(Object result) {
		List<? extends RecordWithVersion<? extends UserRecord>> list = (List<? extends RecordWithVersion<? extends UserRecord>>) result;
		return (RecordWithVersion<R> ) FunctionalUtil.singleResult(list);
	}
	
	public static Envelope query(String queryId, List<?> parameters, TpccTestProcess testProcess) {
		QueryWorkflowInput wfInput = 
				new QueryWorkflowInput(
						queryId, 
						testProcess.txId, 
						testProcess.readVersion, 
						List.of(parameters)
				);
		
		ExecuteWorkflowEvent queryEvent = 
				new ExecuteWorkflowEvent(QueryWorkflow.TYPE, wfInput);
		
		return new Envelope(testProcess.txId.getComputerId(), queryEvent);				
	}

	protected final SQLServerApplication application;
	private final ServerComputerId txComputer;
	
	protected ReadVersion readVersion;
	protected TransactionId txId;

	protected TpccTestProcess(SQLServerApplication application, ServerComputerId txComputer) {
		this.application = application;
		this.txComputer = txComputer;
	}
	
	@Override
	protected ServerComputerId txMaintainingComputer() {
		return txComputer;
	}
	
	protected Result commit(List<Object> results) {
		return of(commitTx(), this::evaluateCommit);
	}
	
	protected Envelope commitTx() {
		return new Envelope(txId.getComputerId(), new CommitTransactionEvent(txId));
	}
	
	protected Result evaluateCommit(Object result) {
		return TestProcess.FINISHED;
	}
	
	protected Envelope modifyEvent(ModificationCollection modificationCollection) {
		ChangeRecordsWorkflowInput wfInput = 
				new ChangeRecordsWorkflowInput(txId, modificationCollection , txId.getComputerId(), readVersion);
		
		ExecuteWorkflowEvent queryEvent = 
				new ExecuteWorkflowEvent(ModifyRecordsWorkflow.TYPE, wfInput);
		
		return new Envelope(txId.getComputerId(), queryEvent);
	}
	
	protected Envelope query(String queryId, List<?> parameters) {
		return query(queryId, parameters, this);
	}

	protected <T extends UserRecord> RecordWithVersion<T> singleRecord(Object returned) {
		@SuppressWarnings("unchecked")
		List<? extends RecordWithVersion<T>> result = (List<? extends RecordWithVersion<T>>) returned;
		
		return FunctionalUtil.singleResult(result);
	}
	
	@Override
	public NextStep start() {
		return NextStep.ofSingle(startTransaction(), this::startInternal);
	}
	
	@SuppressWarnings("unchecked")
	protected Result startInternal(Object result) {
		Pair<TransactionId, ReadVersion> pair = (Pair<TransactionId, ReadVersion>) result;
		txId = pair.getKey();
		readVersion = pair.getValue();

		LOGGER.debug("tx-id {} read-version {}", txId, readVersion);
		
		
		return startTx();
	}

	protected abstract Result startTx();

	
	protected <R extends UserRecord> Envelope update(RecordWithVersion<R> original, R updated) {
		ModificationCollection modificationCollection = new ModificationCollection();
		modificationCollection.addUpdate(original, updated);
		
		return modifyEvent(modificationCollection);		
	}
}
