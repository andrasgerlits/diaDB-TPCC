package com.dianemodb.tpcc;

import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.ServerConfig;
import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.integration.AbstractServerTestCase;
import com.dianemodb.integration.test.runner.AbstractTestRunner;
import com.dianemodb.integration.test.runner.ExampleRunner;
import com.dianemodb.integration.test.runner.InstanceRunner;
import com.dianemodb.kryo.KryoSerializer;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.metaschema.DianemoApplicationImpl;
import com.dianemodb.metaschema.UserRecordTable;
import com.dianemodb.tpcc.schema.CustomerTable;
import com.dianemodb.tpcc.schema.DistrictTable;
import com.dianemodb.tpcc.schema.HistoryTable;
import com.dianemodb.tpcc.schema.ItemTable;
import com.dianemodb.tpcc.schema.NewOrdersTable;
import com.dianemodb.tpcc.schema.OrderLineTable;
import com.dianemodb.tpcc.schema.OrdersTable;
import com.dianemodb.tpcc.schema.StockTable;
import com.dianemodb.tpcc.schema.WarehouseTable;
import com.dianemodb.tpcc.transaction.TpccClientRunner;

public class TpccRunner extends AbstractTestRunner {
	
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TpccRunner.class.getName());
	
	// flip this switch to alternate between running the test or initializing the database
	private static final boolean recreate = false;

	public static DianemoApplication createApplication(Topology topology) {
		
		CustomerTable customerTable = new CustomerTable(topology);
		NewOrdersTable newOrdersTable = new NewOrdersTable(topology);
		OrdersTable ordersTable = new OrdersTable(topology);
		ItemTable itemTable = new ItemTable(topology);
		OrderLineTable orderLineTable = new OrderLineTable(topology);
		WarehouseTable warehouseTable = new WarehouseTable(topology);
		DistrictTable districtTable = new DistrictTable(topology);
		StockTable stockTable = new StockTable(topology);
		HistoryTable historyTable = new HistoryTable(topology);
		
		List<UserRecordTable<? extends UserRecord>> recordTables = 
				List.of(
					customerTable,
					districtTable, 
					historyTable,
					itemTable,
					newOrdersTable,
					orderLineTable,
					ordersTable,
					stockTable,
					warehouseTable
				);
		
		return new DianemoApplicationImpl("tpcc", recordTables, 100, topology, KryoSerializer::new);
	}
	
	public static void main(String[] args) throws Exception {
		new TpccRunner(1, recreate).run();

		//new TpccRunner(1, false).run();
	}
	
	public TpccRunner(int numberOfInstances, boolean reInitData) throws Exception {
		super(numberOfInstances, reInitData, ExampleRunner.SMALL_SINGLE_LEVEL_TOPOLOGY);
	}
	
	@Override
	protected TpccInitializerRunner createInitializer(final String initNodeId) {
		// insert the initial data into the DB
		return FunctionalUtil.doOrPropagate(
			() -> TpccInitializerRunner.init(
					new String[] {
							// use default kafka properties
							"-t", topologyFile,
							"-b", AbstractServerTestCase.getBootstrapUrl(),
							"-id", initNodeId ,
							"-tx", String.valueOf(40)
					}					
				)
			);
	}

	@Override
	protected InstanceRunner createComputerInstanceRunner(int idBase, int i, boolean wipeSchema) {
		return FunctionalUtil.doOrPropagate(() -> 
				InstanceRunner.init(
					new String[] {
						// start everything
						//"-i", "0,1,2", 
						"-t", topologyFile,
						"-" + InstanceRunner.SUBDIRECTORY_SWITCH, ServerConfig.DEFAULT_ROOT_DIRECTORY + "server_" + idBase
					},
					t -> createApplication(t),
					reInitData
				)
			);
	}

	@Override
	protected TpccClientRunner createClient(String idString) {
		if(recreate) {
			return null;
		}
		else {
			return FunctionalUtil.doOrPropagate(
				() -> TpccClientRunner.init(
							new String[] {
								"-t", topologyFile,
								"-id", idString,
								"-b", AbstractServerTestCase.getBootstrapUrl()
							},
							t -> createApplication(t),
							100
					)
			);
		}
	}

}
