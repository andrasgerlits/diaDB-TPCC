package com.dianemodb.tpcc;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.LoggerFactory;

import com.dianemodb.ServerComputerId;
import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.id.TransactionId;
import com.dianemodb.runner.InitializerRunner;
import com.dianemodb.tpcc.init.CustomerInitializer;
import com.dianemodb.tpcc.init.DistrictInitializer;
import com.dianemodb.tpcc.init.ItemInitializer;
import com.dianemodb.tpcc.init.OrderInitializer;
import com.dianemodb.tpcc.init.StockInitializer;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.init.WarehouseInitializer;

public class TpccInitializerRunner extends InitializerRunner {
	
	private static final String NUMBER_OF_TX_SWITCH = "tx";

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TpccInitializerRunner.class.getName());
	
	private final List<TpccDataInitializer> initializers;
	private TpccDataInitializer currentInitializer;

	public TpccInitializerRunner(
			Properties kafkaServerProperties, 
			String bootStrapUrl, 
			ServerComputerId topicId,
			Topology topology, 
			int numberOfTx
	) throws Exception 
	{
		super(
			kafkaServerProperties, 
			bootStrapUrl, 
			topicId, 
			topology, 
			numberOfTx,
			TpccRunner.createApplication(topology)
		);

		this.initializers = new LinkedList<>();
		
		initializers.add(new ItemInitializer(application));
		initializers.add(new WarehouseInitializer(application));
		initializers.add(new DistrictInitializer(application));
		initializers.add(new StockInitializer(application));
		initializers.add(new CustomerInitializer(application));
		initializers.add(new OrderInitializer(application));
		
		this.currentInitializer = initializers.remove(0);
		
		LOGGER.info("Processing " + currentInitializer.getClass().getSimpleName());
	}
	
	public static TpccInitializerRunner init(String[] args) throws Exception {
		Options options = getCommonOptions();
		options.addRequiredOption(
				NUMBER_OF_TX_SWITCH, 
				"tx_number", 
				true, 
				"Number of transactions to use to create records"
		);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		TpccInitializerRunner runner = 
			new TpccInitializerRunner(
					getKafkaServerProperties(cmd), 
					cmd.getOptionValue("b"), 
					getTopicId(cmd), 
					readTopologyFromFile(cmd.getOptionValue("t")),
					Integer.valueOf(cmd.getOptionValue(NUMBER_OF_TX_SWITCH))			
				);
		
		return runner;
	}

	@Override
	protected boolean canGenerateMore() {
		return currentInitializer.hasNext() || !initializers.isEmpty();
	}

	@Override
	protected List<UserRecord> createRecords(TransactionId txId) {
		assert canGenerateMore();
		
		if(!currentInitializer.hasNext()) {
			LOGGER.info("Finished " + currentInitializer.getClass().getSimpleName());
			currentInitializer = initializers.remove(0);
			LOGGER.info("Processing " + currentInitializer.getClass().getSimpleName());
		}
		
		List<UserRecord> records = currentInitializer.process(txId);
		return records;
	}
	

}
