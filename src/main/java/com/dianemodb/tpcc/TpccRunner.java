package com.dianemodb.tpcc;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.integration.AbstractServerTestCase;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.UserRecordQueryStep;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.runner.ClientRunner;
import com.dianemodb.runner.ExampleRunner;
import com.dianemodb.runner.InitializerRunner;
import com.dianemodb.runner.InstanceRunner;
import com.dianemodb.sql.SQLApplicationImpl;
import com.dianemodb.tpcc.query.FindCustomerByLastNameDistrictAndWarehouse;
import com.dianemodb.tpcc.query.FindDistrictByIdAndWarehouse;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderIdRangeDistrictAndWarehouse;
import com.dianemodb.tpcc.query.FindOrderLinesByOrderidDistrictAndWarehouse;
import com.dianemodb.tpcc.query.FindWarehouseDetailsById;
import com.dianemodb.tpcc.query.delivery.FindNewOrderWithLowestOrderIdByDistrictAndWarehouse;
import com.dianemodb.tpcc.query.delivery.FindOrderByDistrictWarehouseOrderId;
import com.dianemodb.tpcc.query.neworder.FindCustomerDetailsByWarehouseAndId;
import com.dianemodb.tpcc.query.neworder.FindItemById;
import com.dianemodb.tpcc.query.neworder.FindStockByItemAndWarehouseId;
import com.dianemodb.tpcc.query.orderstatus.FindMaxOrderIdForCustomer;
import com.dianemodb.tpcc.query.payment.FindCustomerByIdDistrictAndWarehouse;
import com.dianemodb.tpcc.schema.CustomerTable;
import com.dianemodb.tpcc.schema.DistrictTable;
import com.dianemodb.tpcc.schema.HistoryTable;
import com.dianemodb.tpcc.schema.ItemTable;
import com.dianemodb.tpcc.schema.NewOrdersTable;
import com.dianemodb.tpcc.schema.OrderLineTable;
import com.dianemodb.tpcc.schema.OrdersTable;
import com.dianemodb.tpcc.schema.StockTable;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class TpccRunner extends ExampleRunner {

	public static SQLServerApplication createApplication(Topology topology) {
		
		CustomerTable customerTable = new CustomerTable(topology);
		NewOrdersTable newOrdersTable = new NewOrdersTable(topology);
		OrdersTable ordersTable = new OrdersTable(topology);
		ItemTable itemTable = new ItemTable(topology);
		OrderLineTable orderLineTable = new OrderLineTable(topology);
		WarehouseTable warehouseTable = new WarehouseTable(topology);
		DistrictTable districtTable = new DistrictTable(topology);
		StockTable stockTable = new StockTable(topology);
		
		List<UserRecordTable<? extends UserRecord>> recordTables = 
				List.of(
					new CustomerTable(topology),
					districtTable, 
					new HistoryTable(topology),
					itemTable,
					newOrdersTable,
					orderLineTable,
					ordersTable,
					stockTable,
					warehouseTable
				);
		
		Collection<UserRecordQueryStep<?>> queryPlans = 
				List.of(
					new FindNewOrderWithLowestOrderIdByDistrictAndWarehouse(newOrdersTable),
					new FindOrderByDistrictWarehouseOrderId(ordersTable),
					
					new FindCustomerDetailsByWarehouseAndId(customerTable),
					new FindItemById(itemTable),
					new FindStockByItemAndWarehouseId(stockTable),
					
					new FindMaxOrderIdForCustomer(ordersTable),
					
					new FindCustomerByIdDistrictAndWarehouse(customerTable),
					
					new FindCustomerByLastNameDistrictAndWarehouse(customerTable),
					new FindDistrictByIdAndWarehouse(districtTable),
					new FindOrderLinesByOrderidDistrictAndWarehouse(orderLineTable),
					new FindOrderLinesByOrderIdRangeDistrictAndWarehouse(orderLineTable),
					new FindWarehouseDetailsById(warehouseTable)
				);
		
		return new SQLApplicationImpl(recordTables, queryPlans);
	}
	
	public static void main(String[] args) throws Exception {
		new TpccRunner();
	}
	
	public TpccRunner() throws Exception {
		super();
	}
	
	protected String getTopologyFile() {
		return ExampleRunner.SMALL_SINGLE_LEVEL_TOPOLOGY;
	}
	
	protected InitializerRunner createInitializer(int numberOfRecords, final String initNodeId) throws Exception {
		// insert the initial data into the DB
		TpccInitializerRunner initRunner = 
				TpccInitializerRunner.init(
					new String[] {
							// use default kafka properties
							"-t", getTopologyFile(),
							"-b", AbstractServerTestCase.getBootstrapUrl(),
							"-id", initNodeId ,
							"-tx", String.valueOf(2000)
					}					
				);
		
		return initRunner;
	}
	
	@Override
	protected ClientRunner createClient(
			int writerPercent, 
			int txPerClient, 
			int numberOfRecords, 
			String idString
	)
	throws Exception 
	{
		// listen all'a you, sabotage!
		return null;
	}

	protected InstanceRunner createComputerInstanceRunner(CompletableFuture<Void>[] finishers, int idBase, int i) throws Exception {
		InstanceRunner instance =
				InstanceRunner.init(
					new String[] {
						// start everything
						//"-i", "0,1,2", 
						"-t", getTopologyFile(),
						"-" + InstanceRunner.SUBDIRECTORY_SWITCH, "server_" + idBase
					},
					t -> createApplication(t)
				);
		
		return instance;
	}

}
