package com.dianemodb.tpcc.transaction;

import java.util.Properties;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import com.dianemodb.ServerComputerId;
import com.dianemodb.Topology;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.integration.AbstractServerTestCase;
import com.dianemodb.integration.test.ProcessManager;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.runner.AbstractClientRunner;
import com.dianemodb.runner.DiaDBRunner;
import com.dianemodb.runner.ExampleRunner;

public class TpccClientRunner extends AbstractClientRunner {

	public TpccClientRunner(
			Properties kafkaServerProperties, 
			String bootStrapUrl, 
			ServerComputerId topicId,
			Topology topology, 
			SQLServerApplication application
	) throws Exception {
		super(kafkaServerProperties, bootStrapUrl, topicId, topology, application);
	}

	@Override
	protected ProcessManager createTestProcessManager() {
		return new TpccProcessManager(application, topology.getLeafNodes());
	}

	public static TpccClientRunner init(
			String[] args, 
			Function<Topology, SQLServerApplication> f
	) {
		Options options = getCommonOptions();

		CommandLineParser parser = new DefaultParser();
		
		return FunctionalUtil.doOrPropagate( 
			() -> {
				CommandLine cmd = parser.parse(options, args);
				
				String topologyFileName = cmd.getOptionValue("t", ExampleRunner.DEFAULT_TOPOLOGY_JSON);
				String bootstrapUrl = cmd.getOptionValue("b", AbstractServerTestCase.getBootstrapUrl());
				
				Topology topology = readTopologyFromFile(topologyFileName);
				SQLServerApplication application = DiaDBRunner.createApplication(topology);
				
				return new TpccClientRunner(
								getKafkaServerProperties(cmd), 
								bootstrapUrl, 
								getTopicId(cmd), 
								topology, 
								application
						);
			}
		);

	}

}
