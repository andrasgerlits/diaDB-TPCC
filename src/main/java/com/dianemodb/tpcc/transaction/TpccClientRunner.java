package com.dianemodb.tpcc.transaction;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import com.dianemodb.Topology;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.integration.AbstractServerTestCase;
import com.dianemodb.integration.runner.AbstractClientRunner;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.integration.test.runner.ExampleRunner;
import com.dianemodb.metaschema.DianemoApplication;

public class TpccClientRunner extends AbstractClientRunner {

	private int concurrentRequestNumber;
	private final String appId;
	
	public TpccClientRunner(
			Properties kafkaServerProperties, 
			String bootStrapUrl, 
			ServerComputerId topicId,
			Topology topology, 
			DianemoApplication application,
			int concurrentRequestNumber
	) throws Exception {
		super(
			kafkaServerProperties, 
			bootStrapUrl, 
			topicId, 
			topology, 
			List.of(application)
	);
		this.appId = application.getId();
		this.concurrentRequestNumber = concurrentRequestNumber;
	}

	@Override
	protected ProcessManager createTestProcessManager() {
		return new TpccProcessManager(
						application(appId), 
						topology.getLeafNodes(), 
						concurrentRequestNumber
					);
	}

	public static TpccClientRunner init(
			String[] args, 
			Function<Topology, DianemoApplication> f,
			int parallelRequestNumber
	) {
		Options options = getCommonOptions();

		CommandLineParser parser = new DefaultParser();
		
		return FunctionalUtil.doOrPropagate( 
			() -> {
				CommandLine cmd = parser.parse(options, args);
				
				String topologyFileName = cmd.getOptionValue("t", ExampleRunner.DEFAULT_TOPOLOGY_JSON);
				String bootstrapUrl = cmd.getOptionValue("b", AbstractServerTestCase.getBootstrapUrl());
				
				Topology topology = Topology.readTopologyFromJsonFile(topologyFileName);

				return new TpccClientRunner(
								getKafkaServerProperties(cmd), 
								bootstrapUrl, 
								getTopicId(cmd), 
								topology, 
								f.apply(topology),
								parallelRequestNumber
						);
			}
		);

	}

}
