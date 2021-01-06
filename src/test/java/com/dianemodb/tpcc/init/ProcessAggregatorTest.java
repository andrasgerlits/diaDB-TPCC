package com.dianemodb.tpcc.init;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.dianemodb.ConversationId;
import com.dianemodb.ServerComputerId;
import com.dianemodb.ServerEvent;
import com.dianemodb.Topology;
import com.dianemodb.event.ExecuteWorkflowEvent;
import com.dianemodb.event.tx.RollbackTransactionEvent;
import com.dianemodb.event.tx.StartTransactionEvent;
import com.dianemodb.exception.ClientInitiatedRollbackTransactionException;
import com.dianemodb.functional.ByteUtil;
import com.dianemodb.id.TransactionId;
import com.dianemodb.integration.test.ProcessAggregator;
import com.dianemodb.integration.test.TestProcess;
import com.dianemodb.integration.test.TestProcess.Result;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.TpccRunner;
import com.dianemodb.tpcc.transaction.TpccTestProcess;
import com.dianemodb.version.ReadVersion;
import com.dianemodb.workflow.query.QueryWorkflowInput;

import fj.data.Either;

public class ProcessAggregatorTest {
	
	private static final ServerComputerId OTHER_COMPUTER_ID = ServerComputerId.valueOf("1");

	private static final ServerComputerId COMPUTER_ID = ServerComputerId.valueOf("0"); 
	
	private static final List<ServerComputerId> COMPUTERS =
			List.of(COMPUTER_ID, OTHER_COMPUTER_ID);
	
	private static final Topology TOPOLOGY = new Topology(Map.of(ServerComputerId.ROOT, COMPUTERS));
	
	private static final SQLServerApplication APPLICATION = TpccRunner.createApplication(TOPOLOGY);
	
	private static final Random RANDOM = new Random();
	
	private static final TransactionId TX_ID = TransactionId.valueOf("1:123");
	
	private static final ReadVersion READ_VERSION = new ReadVersion(List.of(100L), ServerComputerId.ROOT);

	private static final ConversationId CONVERSATION_ID = ConversationId.parse("1:123:asdfasd");

	@Test
	public void testClientRollbackBeingSent() throws Exception {
		ProcessAggregator aggregator = 
			new ProcessAggregator(100) {
				@Override
				protected void success(TestProcess process) {
					// ignore
				}
	
				@Override
				protected Result failed(ConversationId conversationId, Throwable ex, TestProcess process) {
					throw new IllegalStateException();
				}
			};
/*			
			ExecuteWorkflowEvent event = 
					new ExecuteWorkflowEvent(
							"workflowTypeKey", 
							new QueryWorkflowInput("queryID", TX_ID, READ_VERSION, List.of())
					);
			
			Envelope envelope = new Envelope(OTHER_COMPUTER_ID, event);
*/			
			TpccTestProcess process = 
					new TpccTestProcess(
							RANDOM, 
							APPLICATION, 
							COMPUTER_ID, 
							2000, 
							5000, 
							(short) 5, 
							ByteUtil.randomStringUUIDMostSignificant()
					) {
						@Override
						protected Result startTx() {
							throw new ClientInitiatedRollbackTransactionException("foo");
						}
					};
					
			aggregator.queueNextSteps(List.of(process.start()));
			
			// the first message being sent will be the start-tx event
			assertSingleEnvelopeBeingSentOfType(
					aggregator, 
					StartTransactionEvent.class, 
					Optional.empty()
			);

			// make it roll back the TX
			aggregator.receiveResults(
					Map.of(
						CONVERSATION_ID, 
						Either.left(Pair.of(TX_ID, READ_VERSION))
					)
			);
			
			// it should have a rollback message waiting to be sent
			assertSingleEnvelopeBeingSentOfType(
					aggregator, 
					RollbackTransactionEvent.class, 
					Optional.of(TX_ID)
			);

			// make it roll back the TX
			aggregator.receiveResults(
					Map.of(
						CONVERSATION_ID, 
						Either.left(Pair.of(TX_ID, READ_VERSION))
					)
			);
	}

	private void assertSingleEnvelopeBeingSentOfType(
			ProcessAggregator aggregator, 
			Class<?> cls,
			Optional<TransactionId> maybeTxId
	) {
		AtomicBoolean hasFlushed = new AtomicBoolean(false);
		aggregator.flushMessages(
				l -> {
					// it must only have the same envelope
					Assert.assertEquals(1, l.size());
					Envelope envelopeBeingSent = l.iterator().next();
					
					maybeTxId.ifPresent(
						txId ->
							{
								ServerEvent payload = envelopeBeingSent.getPayload();
								
								Assert.assertEquals(
										txId, 
										payload.getMaybeTxId().get()
								);
							}
					);
					
					Assert.assertEquals(
							cls, 
							envelopeBeingSent.getPayload().getClass()
					);
					
					hasFlushed.set(true);
					
					return List.of(CONVERSATION_ID);
				}
			);
		
		// must have been called synchronously
		Assert.assertTrue(hasFlushed.get());
	}
}
