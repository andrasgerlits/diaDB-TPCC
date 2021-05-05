package com.dianemodb.tpcc.transaction;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.UserRecord;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.integration.test.BaseProcess;
import com.dianemodb.integration.test.NextStep;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.query.CustomerSelectionById;
import com.dianemodb.tpcc.query.CustomerSelectionByLastName;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.version.Transaction.State;
import com.dianemodb.workflow.tx.TxEndValue;

public abstract class TpccTestProcess extends BaseProcess {
	
	static final Logger LOGGER = LoggerFactory.getLogger(TpccTestProcess.class.getName());
	
	public static final Map<Class<? extends TpccTestProcess>, Integer> MAX_TIMES_BY_CLASS = 
			Map.of(
					Delivery.class, 5000,
					NewOrder.class, 5000,
					OrderStatus.class, 5000,
					Payment.class, 5000,
					StockLevel.class, 20000
			);

	public static final Comparator<NextStep> REQUEST_START_TIME_COMPARATOR = 
			(one, other) -> {
				TpccTestProcess p1 = (TpccTestProcess) one.getProcess();
				TpccTestProcess p2 = (TpccTestProcess) other.getProcess();

				// the ones with the higher value come later
				return Long.compare(
						p1.initialRequestedMinStartTime,						
						p2.initialRequestedMinStartTime
					);
			};

	protected static boolean hasString(String value, String searched) {
		return value != null && value.contains(searched);
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
	        int l = TpccDataInitializer.randomInt(0, 1023);
	        int r = TpccDataInitializer.randomInt(1, (Constants.CUSTOMER_PER_DISTRICT));
	        int C = TpccDataInitializer.randomInt(0, 1023);
	        
	        int customerId = ((l | r) + C) % Constants.CUSTOMER_PER_DISTRICT;
	        
			return new CustomerSelectionById(warehouseId, districtId, customerId);
		}
	}

	protected final Random random;
	protected final DianemoApplication application;
	private final ServerComputerId txComputer;
	protected final short terminalWarehouseId;
	
	/**
	 * Keying time is the time spent before the first request is made from the 
	 * client to the server after the terminal has been taken out of the pool.
	 * 
	 * This is the epoch time at which point the first request can be made for 
	 * the process, so "creation time + keying time".
	 * 
	 * This is also the time from which the latency of the response is calculated.
	 * */
	private long initialRequestedMinStartTime;
	
	/**
	 * Think time is the time spent after the answer was received by the client
	 * and before the terminal was returned to the free pool. 
	 */
	private int thinkTimeInMs;
	
	protected long finished = -1;
	
	private int retryCount = 0;
	
	protected final String uuid;
	
	protected TpccTestProcess(
			Random random, 
			DianemoApplication application, 
			ServerComputerId txComputer, 
			int keyingTimeInMs, 
			int meanThinkTimeInMs,
			short warehouseId, 
			String uuid
	) {
		this.random = random;
		this.application = application;
		this.txComputer = txComputer;

		this.terminalWarehouseId = warehouseId;
		
		int thinkTime = (int) (-1 * Math.log(random.nextDouble()) * meanThinkTimeInMs);
		int maxThinkTime = meanThinkTimeInMs * 10;
		if(thinkTime > maxThinkTime) {
			thinkTime = maxThinkTime;
		}
		this.thinkTimeInMs = thinkTime;
		
		this.initialRequestedMinStartTime = keyingTimeInMs + System.currentTimeMillis();
		
		this.uuid = uuid;
	}
	
	public boolean isTerminalBased() {
		return true;
	}
	
	@Override
	protected Result startInternal(Object result) {
		Result res = super.startInternal(result);
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"{} starting w_id {} tx-id {} read-version {}",
					uuid,
					terminalWarehouseId, 
					txId, 
					readVersion
			);
		}
		
		return res;
	}

	public NextStep start() {
		return ofSingle(
				startTransaction(), 
				this::startInternal, 
				initialRequestedMinStartTime
			);
	}

	public boolean isLate() {
		long maxLatency = MAX_TIMES_BY_CLASS.get(this.getClass());;
		if(finished == -1) { 
			return System.currentTimeMillis() > initialRequestedMinStartTime + maxLatency; 
		}
		else {
			return getTpccLatency() > maxLatency ;
		}
	}
	
	public int txLatency() {
		if(finished == -1) {
			throw new IllegalStateException("Tx not yet finished");
		}
		return (int) (finished - startTime); 
	}
	
	public long getTpccLatency() {
		if(finished == -1) {
			throw new IllegalStateException("Tx not yet finished");
		}
		return (int) (finished - initialRequestedMinStartTime); 
	}

	
	@Override
	protected ServerComputerId txMaintainingComputer() {
		return txComputer;
	}
	
	protected Result commit(List<Object> results) {
		return of(commitTx(), this::evaluateCommit);
	}
	
	protected Result evaluateCommit(Object result) {
		TxEndValue txEndState = ((Optional<TxEndValue>) result).get();
		
		if(txEndState.getTxEndState() == State.COMMITTED) {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"{} committed warehouse-id {} read-version {}", 
						terminalWarehouseId, 
						txId, 
						readVersion
				);
			}
			return BaseProcess.FINISHED;
		}
		else {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"{} commit failed, retrying warehouse-id {} read-version {}", 
						terminalWarehouseId, 
						txId, 
						readVersion
				);
			}
			assert txEndState.getTxEndState() == State.CANCELLED;
			return of(cancelAndRetry());
		}
	}
	
	// TX is rolled back when it receives an exception
	public NextStep cancelAndRetry() {
		retryCount++;
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("{} retrying {}", startTime);
		}
		return ofSingle(startTransaction(), this::startInternal, 0L);
	}
	
	public int getRetryCount() {
		return retryCount;
	}
	
	protected <R extends UserRecord> Envelope update(RecordWithVersion<R> original, R updated) {
		ModificationCollection modificationCollection = new ModificationCollection();
		modificationCollection.addUpdate(original, updated, application);
		
		return modifyEvent(modificationCollection);		
	}
	
	public long getMinInitialRequestTime() {
		return initialRequestedMinStartTime;
	}
	
	public long getThinkTimeInMs() {
		return thinkTimeInMs;
	}

	public Short getWarehouseId() {
		return terminalWarehouseId;
	}
	
	public void finished() {
		assert finished == -1;
		finished = System.currentTimeMillis();
	}
	
	@Override
	public void stepDone(long now, NextStep nextStep) {
		LOGGER.debug(
				"{} Step finished {} {}", 
				uuid, 
				now - startTime, 
				nextStep
		);
		
		super.stepDone(now, nextStep);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " ["
				+ "uuid=" + uuid
				+ ", txComputer=" + txComputer
				+ ", terminalWarehouseId=" + terminalWarehouseId
				+ ", retryCount=" + retryCount
				+ ", initialRequestStartTime=" + initialRequestedMinStartTime 
				+ ", thinkTimeInMs=" + thinkTimeInMs
				+ ", startTime=" + startTime 
			+ "]";
	}

	@Override
	public int hashCode() {
		return uuid.hashCode();
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
		
		return other.uuid.equals(this.uuid);
	}
}
