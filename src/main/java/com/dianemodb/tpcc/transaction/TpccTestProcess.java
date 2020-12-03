package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Map;
import java.util.Random;

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
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.CustomerSelectionById;
import com.dianemodb.tpcc.query.CustomerSelectionByLastName;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.version.ReadVersion;
import com.dianemodb.workflow.query.QueryWorkflow;
import com.dianemodb.workflow.query.QueryWorkflowInput;
import com.dianemodb.workflow.write.ChangeRecordsWorkflowInput;
import com.dianemodb.workflow.write.ModifyRecordsWorkflow;

public abstract class TpccTestProcess extends TestProcess {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TpccTestProcess.class.getName());
	
	public static final Map<Class<? extends TpccTestProcess>, Integer> MAX_TIMES_BY_CLASS = 
			Map.of(
					Delivery.class, 5000,
					NewOrder.class, 5000,
					OrderStatus.class, 5000,
					Payment.class, 5000,
					StockLevel.class, 20000
			);

	protected static boolean hasString(String value, String searched) {
		return value != null && value.contains(searched);
	}
	
	@SuppressWarnings("unchecked")
	public static <R extends UserRecord> RecordWithVersion<R> singleFromResultList(Object result) {
		if(result instanceof List) {
			List<? extends RecordWithVersion<? extends UserRecord>> list = (List<? extends RecordWithVersion<? extends UserRecord>>) result;
			return (RecordWithVersion<R>) FunctionalUtil.singleResult(list);
		}
		else {
			assert result instanceof RecordWithVersion : result; 
			return (RecordWithVersion<R>) result;
		}
	}
	
	protected static CustomerSelectionStrategy randomStrategy(
			Random random, 
			short warehouseId, 
			byte districtId
	) {
		// 0-5 -> 60%
		if(random.nextInt(10) < 6) {
			String randomLastName = TpccDataInitializer.randomLastName();
			return new CustomerSelectionByLastName(randomLastName, warehouseId, districtId);
		}
		else {
			int customerId = random.nextInt(Constants.CUSTOMER_PER_DISTRICT);
			return new CustomerSelectionById(warehouseId, districtId, customerId);
		}
	}

	
	public static Envelope query(
			String queryId, 
			List<?> parameters, 
			TpccTestProcess testProcess
	) {
		QueryWorkflowInput wfInput = 
				new QueryWorkflowInput(
						queryId, 
						testProcess.txId, 
						testProcess.readVersion, 
						parameters
				);
		
		ExecuteWorkflowEvent queryEvent = 
				new ExecuteWorkflowEvent(QueryWorkflow.TYPE, wfInput);
		
		return new Envelope(testProcess.txId.getComputerId(), queryEvent);				
	}

	protected final Random random;
	protected final SQLServerApplication application;
	private final ServerComputerId txComputer;
	private final int maxTimeInMs;
	protected final short terminalWarehouseId;
	
	/**
	 * Keying time is the time spent before the first request is made from the 
	 * client to the server after the terminal has been taken out of the pool.
	 * 
	 * This is the epoch time at which point the first request can be made for 
	 * the process, so "creation time + keying time" 
	 * */
	private final long initialRequestStartTime;
	
	/**
	 * Think time is the time spent after the answer was received by the client
	 * and before the terminal was returned to the free pool. 
	 */
	private final int thinkTimeInMs;
	
	protected final long startTime;
	
	protected final String uuid;
	
	protected TpccTestProcess(
			Random random, 
			SQLServerApplication application, 
			ServerComputerId txComputer, 
			int keyingTimeInMs, 
			int meanThinkTimeInMs,
			short warehouseId, 
			String uuid
	) {
		this.random = random;
		this.application = application;
		this.txComputer = txComputer;
		this.maxTimeInMs = MAX_TIMES_BY_CLASS.get(getClass());

		this.terminalWarehouseId = warehouseId;
		
		int thinkTime = (int) (-1 * Math.log(random.nextDouble()) * meanThinkTimeInMs);
		int maxThinkTime = meanThinkTimeInMs * 10;
		if(thinkTime > maxThinkTime) {
			thinkTime = maxThinkTime;
		}
		this.thinkTimeInMs = thinkTime;
		
		this.startTime = System.currentTimeMillis();
		this.initialRequestStartTime = keyingTimeInMs + startTime;
		
		this.uuid = uuid;
	}
	
	public boolean isTerminalBased() {
		return true;
	}
	
	public boolean isLate() {
		return System.currentTimeMillis() - startTime < maxTimeInMs;
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
		LOGGER.debug("committed warehouse-id {} tx-id {} read-version {}", terminalWarehouseId, txId, readVersion);
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
		return ofSingle(startTransaction(), this::startInternal, initialRequestStartTime);
	}

	// TX is rolled back when it receives an exception
	public NextStep cancelAndRetry() {
		return ofSingle(startTransaction(), this::startInternal, 0L);
	}
	
	@SuppressWarnings("unchecked")
	protected Result startInternal(Object result) {
		Pair<TransactionId, ReadVersion> pair = (Pair<TransactionId, ReadVersion>) result;
		txId = pair.getKey();
		readVersion = pair.getValue();

		LOGGER.debug("started warehouse-id {} tx-id {} read-version {}", terminalWarehouseId, txId, readVersion);
		
		return startTx();
	}

	protected abstract Result startTx();
	
	protected <R extends UserRecord> Envelope update(RecordWithVersion<R> original, R updated) {
		ModificationCollection modificationCollection = new ModificationCollection();
		modificationCollection.addUpdate(original, updated);
		
		return modifyEvent(modificationCollection);		
	}
	
	public long getInitialRequestStartTime() {
		return initialRequestStartTime;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getThinkTimeInMs() {
		return thinkTimeInMs;
	}

	public Short getWarehouseId() {
		return terminalWarehouseId;
	}

	public String getUiid() {
		return uuid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (initialRequestStartTime ^ (initialRequestStartTime >>> 32));
		result = prime * result + maxTimeInMs;
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		result = prime * result + terminalWarehouseId;
		result = prime * result + thinkTimeInMs;
		result = prime * result + ((txComputer == null) ? 0 : txComputer.hashCode());
		result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TpccTestProcess other = (TpccTestProcess) obj;
		if (initialRequestStartTime != other.initialRequestStartTime)
			return false;
		if (maxTimeInMs != other.maxTimeInMs)
			return false;
		if (startTime != other.startTime)
			return false;
		if (terminalWarehouseId != other.terminalWarehouseId)
			return false;
		if (thinkTimeInMs != other.thinkTimeInMs)
			return false;
		if (txComputer == null) {
			if (other.txComputer != null)
				return false;
		} else if (!txComputer.equals(other.txComputer))
			return false;
		if (uuid == null) {
			if (other.uuid != null)
				return false;
		} else if (!uuid.equals(other.uuid))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " ["
				+ "txComputer=" + txComputer
				+ ", maxTimeInMs=" + maxTimeInMs 
				+ ", terminalWarehouseId=" + terminalWarehouseId
				+ ", initialRequestStartTime=" + initialRequestStartTime 
				+ ", thinkTimeInMs=" + thinkTimeInMs
				+ ", startTime=" + startTime 
				+ ", uuid=" + uuid 
			+ "]";
	}
	
	
}
