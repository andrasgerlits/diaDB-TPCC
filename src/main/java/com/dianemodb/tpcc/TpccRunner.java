package com.dianemodb.tpcc;

import java.util.Collection;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.functional.FunctionalUtil;
import com.dianemodb.integration.AbstractServerTestCase;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.UserRecordQueryStep;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.runner.AbstractTestRunner;
import com.dianemodb.runner.ExampleRunner;
import com.dianemodb.runner.InstanceRunner;
import com.dianemodb.runner.KafkaClientRunner;
import com.dianemodb.sql.SQLApplicationImpl;
import com.dianemodb.tpcc.query.FindCustomerByWarehouseDistrictLastName;
import com.dianemodb.tpcc.query.FindDistrictByWarehouseAndDistrictId;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderIdRangeDistrictAndWarehouse;
import com.dianemodb.tpcc.query.FindOrderLinesByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.FindWarehouseDetailsById;
import com.dianemodb.tpcc.query.delivery.FindNewOrderWithLowestOrderIdByWarehouseAndDistrict;
import com.dianemodb.tpcc.query.delivery.FindOrderByWarehouseDistrictOrderId;
import com.dianemodb.tpcc.query.neworder.FindItemById;
import com.dianemodb.tpcc.query.neworder.FindStockByWarehouseItem;
import com.dianemodb.tpcc.query.orderstatus.FindMaxOrderIdForCustomer;
import com.dianemodb.tpcc.query.payment.FindCustomerByWarehouseDistrictAndId;
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

	public static SQLServerApplication createApplication(Topology topology) {
		
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
		
		Collection<UserRecordQueryStep<?>> queryPlans = 
				List.of(
					new FindNewOrderWithLowestOrderIdByWarehouseAndDistrict(newOrdersTable),
					new FindOrderByWarehouseDistrictOrderId(ordersTable),
					
					new FindItemById(itemTable),
					new FindStockByWarehouseItem(stockTable),
					
					new FindMaxOrderIdForCustomer(ordersTable),
					
					new FindCustomerByWarehouseDistrictAndId(customerTable),
					
					new FindCustomerByWarehouseDistrictLastName(customerTable),
					new FindDistrictByWarehouseAndDistrictId(districtTable),
					new FindOrderLinesByWarehouseDistrictOrderId(orderLineTable),
					new FindOrderLinesByOrderIdRangeDistrictAndWarehouse(orderLineTable),
					new FindWarehouseDetailsById(warehouseTable)
				);
		
		return new SQLApplicationImpl(recordTables, queryPlans);
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
							"-tx", String.valueOf(1000)
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
						"-" + InstanceRunner.SUBDIRECTORY_SWITCH, KafkaClientRunner.ABSOLUTE_ROOT_DIRECTORY + "server_" + idBase
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
							48
					)
			);
		}
	}

}
