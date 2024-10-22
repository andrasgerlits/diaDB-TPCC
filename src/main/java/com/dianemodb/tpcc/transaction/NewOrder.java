package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianemodb.ModificationCollection;
import com.dianemodb.QueryDefinition;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.exception.ClientInitiatedRollbackTransactionException;
import com.dianemodb.id.ServerComputerId;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.DianemoApplication;
import com.dianemodb.metaschema.distributed.Condition;
import com.dianemodb.metaschema.distributed.Operator;
import com.dianemodb.tpcc.Constants;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.entity.Item;
import com.dianemodb.tpcc.entity.NewOrders;
import com.dianemodb.tpcc.entity.OrderLine;
import com.dianemodb.tpcc.entity.Orders;
import com.dianemodb.tpcc.entity.Stock;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.init.TpccDataInitializer;
import com.dianemodb.tpcc.schema.CustomerTable;
import com.dianemodb.tpcc.schema.DistrictTable;
import com.dianemodb.tpcc.schema.ItemTable;
import com.dianemodb.tpcc.schema.StockTable;
import com.dianemodb.tpcc.schema.WarehouseTable;


public class NewOrder extends TpccTestProcess {

	private static final int INVALID_ITEM_ID = Constants.ITEM_NUMBER * 2;
	private static final Logger LOGGER = LoggerFactory.getLogger(NewOrder.class.getName());
	
	private static Function<Stock, String> getDistrictInfo(byte districtNumber) {
		// district-ids are 0-based
		switch(districtNumber + 1) {
	        case 1: return s ->  s.getDist1();
	        case 2: return s ->  s.getDist2();
	        case 3: return s ->  s.getDist3();
	        case 4: return s ->  s.getDist4();
	        case 5: return s ->  s.getDist5();
	        case 6: return s ->  s.getDist6();
	        case 7: return s ->  s.getDist7();
	        case 8: return s ->  s.getDist8();
	        case 9: return s ->  s.getDist9();
	        case 10: return s ->  s.getDist10();
	        default: throw new IllegalArgumentException(String.valueOf(districtNumber));
		}        	
   	}
	
	private static boolean isUsedItemIdValue(int itemId) {
		// higher than the highest ID
		return itemId > Constants.ITEM_NUMBER - 1;
	}
	
	private final int customerId;
	private final byte customerDistrictId;
	private final Map<Integer, Pair<Short, Short>> supplyingWarehouseAndQuantityByItemId;
	
	public NewOrder(
			Random random, 
			ServerComputerId txComputer,
			DianemoApplication application,

			short homeWarehouseId,
			byte homeDistrictId,
			
			String uuid
	) {
		super(random, application, txComputer, 18000, 12000, homeWarehouseId, uuid);
		
		this.customerId = TpccDataInitializer.randomCustomerId();
		this.customerDistrictId = homeDistrictId;
		
		int orderLineCount = TpccDataInitializer.randomInt(5, 15);
		
		boolean wrongItemInput = random.nextInt(100) == 0;
		
		supplyingWarehouseAndQuantityByItemId = new HashMap<>();
		
		for(int i = 0; i < orderLineCount; i++) {
			short warehouseId = random.nextInt(100) == 0 ? 
					TpccDataInitializer.randomWarehouseId() 
					: homeWarehouseId;
			
			int itemId;
			if(i == orderLineCount - 1 && wrongItemInput) {
				itemId = INVALID_ITEM_ID;
			}
			else {
				itemId = TpccDataInitializer.randomItemId();
			}
			
			assert itemId >= 0 : itemId;
			
			short quantity = (short) (random.nextInt(10) + 1);
			
			supplyingWarehouseAndQuantityByItemId.put(itemId, Pair.of(warehouseId, quantity));
		}
	}
	
	public static final QueryDefinition<Customer> FIND_CUSTOMER_BY_WH_DIST_ID = 
			new QueryDefinition<Customer>(
					CustomerTable.ID, 
					new Condition<>(
						List.of(
							Pair.of(CustomerTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
							Pair.of(CustomerTable.DISTRICT_ID_COLUMN, Operator.EQ),
							Pair.of(CustomerTable.ID_COLUMN, Operator.EQ)
						)
					),
					false
			);
	
	public static final QueryDefinition<Warehouse> FIND_WAREHOUSE_BY_ID = 
			new QueryDefinition<>(
					WarehouseTable.ID, 
					new Condition<>(WarehouseTable.ID_COLUMN, Operator.EQ),
					false
			);
	
	public static final QueryDefinition<District> FIND_DISTRICT_BY_WH_DIST_ID = 
			new QueryDefinition<>(
					DistrictTable.ID, 
					new Condition<>(
						List.of(
							Pair.of(DistrictTable.WAREHOUSE_COLUMN, Operator.EQ),
							Pair.of(DistrictTable.ID_COLUMN, Operator.EQ)
						)
					),
					false
			);
	
	public static final QueryDefinition<Item> FIND_ITEM_BY_ID = 
			new QueryDefinition<>(
					ItemTable.ID, 
					new Condition<>(ItemTable.ID_COLUMN, Operator.EQ),
					true
			);
	
	
	public static final QueryDefinition<Stock> FIND_STOCK_BY_WH_ITEM_ID =
			new QueryDefinition<>(
					StockTable.ID,
					new Condition<>(
						List.of(
							Pair.of(StockTable.WAREHOUSE_ID_COLUMN, Operator.EQ),
							Pair.of(StockTable.ITEM_ID_COLUMN, Operator.EQ)
						)
					),
					true
			);
	
	@Override
	protected Result startTx() {		
		Envelope queryCustomerEnvelope = 
				query(
					FIND_CUSTOMER_BY_WH_DIST_ID, 
					// query is multiparam-based
					List.of(
						List.of(terminalWarehouseId, customerDistrictId, customerId)
					) 
				);

		Envelope queryWarehouseEnvelope = 
				query(
					FIND_WAREHOUSE_BY_ID, 
					List.of(terminalWarehouseId)
				);
		
		Envelope queryDistrictEnvelope =
				query(
					FIND_DISTRICT_BY_WH_DIST_ID, 
					List.of(terminalWarehouseId, customerDistrictId)
				);
		
		ItemTable itemTable = (ItemTable) application.getTableById(ItemTable.ID);
		short distId = itemTable.getDistributionIndexForServer(txId.getComputerId());
		
		/*
		 *  since items are duplicated for each warehouse, this will only be
		 *  a single query on the machine running the transaction. 
		 */
		Envelope queryItemEnvelope = 
				query(
					FIND_ITEM_BY_ID, 
					new ArrayList<>(
						/*
						 * warehouse only added for efficiency, collecting as a set, since
						 * ordering is not imprtant, but we could potentially have the same
						 * item for different lines.
						 */
						supplyingWarehouseAndQuantityByItemId.keySet()
							.stream()
							.map(itemId -> List.of(distId, itemId))
							.collect(Collectors.toSet())
					)
				);

		// select stock for each item
		List<List<? extends Object>> itemWarehouseIds = 
				supplyingWarehouseAndQuantityByItemId.entrySet()
					.stream()
					// warehouse-id, item-id
					.map( e -> new LinkedList<>(List.of(e.getValue().getKey(), e.getKey())))
					.collect(Collectors.toList());
		
		Envelope queryStocksEnvelope = query(FIND_STOCK_BY_WH_ITEM_ID, itemWarehouseIds);

		return of(
				List.of(
					queryCustomerEnvelope, 
					queryWarehouseEnvelope, 
					queryDistrictEnvelope,
					queryItemEnvelope, 
					queryStocksEnvelope
				), 
				this::updateDistrictInsertNewOrdersQueryStocks
		);
	}
	
	@SuppressWarnings("unchecked")
	private Result updateDistrictInsertNewOrdersQueryStocks(List<Object> results) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		ModificationCollection modificationCollection = new ModificationCollection();
				
		Iterator<Object> resultIter = results.iterator();
		
		RecordWithVersion<Customer> customer = singleFromResultList(resultIter.next());
		RecordWithVersion<Warehouse> warehouse = singleFromResultList(resultIter.next());
		RecordWithVersion<District> district = singleFromResultList(resultIter.next());
		
		List<RecordWithVersion<Item>> items = (List<RecordWithVersion<Item>>) resultIter.next();
		if(items.size() < supplyingWarehouseAndQuantityByItemId.size()) {
			if(!supplyingWarehouseAndQuantityByItemId.containsKey(INVALID_ITEM_ID)) {
				throw new IllegalStateException("Fewer items returned than expected");
			}
			else {
				throw new ClientInitiatedRollbackTransactionException(uuid);
			}
		}
		
		List<RecordWithVersion<Stock>> stocks = (List<RecordWithVersion<Stock>>) resultIter.next();
		
		
		District oldDistrict = district.getRecord();
		
		int orderId = oldDistrict.getNextOid();
		
		District updatedDistrict = oldDistrict.shallowClone(application, txId);		
		updatedDistrict.setNextOid(orderId + 1);
				
		modificationCollection.addUpdate(district, updatedDistrict, application);
		
		Orders order = new Orders(txId, null);
		order.setOrderId(orderId);
		order.setDistrictId(customerDistrictId);
		order.setWarehouseId(terminalWarehouseId);
		order.setCustomerId(customerId);
		order.setEntryDate(timestamp);
		
		order.setOrderLineCount( (short) supplyingWarehouseAndQuantityByItemId.size() );
		order.setAllLocal( (short) ( allOrderLocal() ? 1 : 0 ) );
		
		modificationCollection.addInsert(order);
		
		NewOrders newOrder = new NewOrders(txId, null);
		newOrder.setOrderId(orderId);
		newOrder.setDistrictId(customerDistrictId);
		newOrder.setWarehouseId(terminalWarehouseId);
		
		modificationCollection.addInsert(newOrder);

		// test that each queried item was found. if not, roll back TX, unused item#
		assert CollectionUtils.isEqualCollection(
					items.stream()
						.map( rwv -> rwv.getRecord().getItemId())
						.collect(Collectors.toSet()),
					supplyingWarehouseAndQuantityByItemId.keySet()
				);
		
		boolean unusedIdFound = items.stream().allMatch(i -> isUsedItemIdValue(i.getRecord().getItemId()));
				
		if(unusedIdFound) {
			throw new ClientInitiatedRollbackTransactionException("2.4.1.5-1");
		}
		
		Map<Integer, Item> itemsById = 
				items.stream()
					.map(RecordWithVersion::getRecord)
					.collect(
						Collectors.toMap(
								(Item i) -> i.getItemId(), 
								i -> i
						)
					);
		
		addModifiedStockRecords(
				orderId, 
				stocks, 
				itemsById, 
				modificationCollection,
				warehouse.getRecord(),
				district.getRecord(),
				customer.getRecord()
		);
		
		Envelope updateEnvelope = modifyEvent(modificationCollection);
		
		return of(List.of(updateEnvelope), this::commit);
	}

	private void addModifiedStockRecords(
			int orderId,
			List<RecordWithVersion<Stock>> stocks,
			Map<Integer, Item> dbItemsById,
			ModificationCollection modificationCollection,
			Warehouse warehouse, 
			District district,
			Customer customer			
	) {
		// sort each list into maps, with their warehouse and item-id being the key for easy association
		Map<Pair<Integer, Short>, RecordWithVersion<Stock>> stockByItemAndWarehouseId = 
				stocks.stream()
					.collect(
						Collectors.toMap( 
							rwv -> Pair.of(
									rwv.getRecord().getItemId(), 
									rwv.getRecord().getWarehouseId()
								), 
							i -> i
						)
					);
		
		if(stockByItemAndWarehouseId.size() != supplyingWarehouseAndQuantityByItemId.size()) {
			throw new IllegalStateException();
		}
		
		Map<Pair<Integer, Short>, Item> itemByIdAndWarehouseId = 
				supplyingWarehouseAndQuantityByItemId.entrySet()
					.stream()
					.collect(
						Collectors.toMap(
							(Entry<Integer, Pair<Short,Short>> e) -> {
								Integer itemId = e.getKey();
								Short warehouseId = e.getValue().getKey();
								return Pair.of(itemId, warehouseId);
							},
							(Entry<Integer, Pair<Short,Short>> e) -> {
								Integer itemId = e.getKey();
								Item item = dbItemsById.get(itemId);
								return item;
							}
						)
					);
		
		if(itemByIdAndWarehouseId.size() != supplyingWarehouseAndQuantityByItemId.size()) {
			throw new IllegalStateException();
		}
		
		Iterator<Entry<Pair<Integer, Short>, RecordWithVersion<Stock>>> stockIter = 
				stockByItemAndWarehouseId.entrySet().iterator();
		
		for(short i = 0; stockIter.hasNext(); i++) {
			Entry<Pair<Integer, Short>, RecordWithVersion<Stock>> stockEntry = stockIter.next();
			
			RecordWithVersion<Stock> originalStockRecord = stockEntry.getValue();
			Stock originalStock = originalStockRecord.getRecord();
			
			// get the item with the same supplying warehouse and item-id as the one in stock 
			Item item = itemByIdAndWarehouseId.get(stockEntry.getKey());
			
			boolean isBrand = 
					hasString(item.getData(), Constants.BRAND_SIGNAL_KEY) 
						&& hasString(originalStock.getData(), Constants.BRAND_SIGNAL_KEY);
			
			short supplyingWarehouse = stockEntry.getKey().getValue();
			short quantity = supplyingWarehouseAndQuantityByItemId.get(item.getItemId()).getValue();
			
			short newStockQuantity = (short) (originalStock.getQuantity() - quantity);
			
			if(newStockQuantity <= 0) {
				newStockQuantity =  (short) (newStockQuantity + 91);
			}
			
			Stock updatedStock = originalStock.shallowClone(application, txId);		
			updatedStock.setQuantity(newStockQuantity);
			
			modificationCollection.addUpdate(originalStockRecord, updatedStock, application);
			BigDecimal amount = 
					new BigDecimal(originalStock.getQuantity())
						.multiply(item.getPrice())
						.multiply( new BigDecimal(1).add(warehouse.getTax()).add(district.getTax()) )
						.multiply( new BigDecimal(1).subtract(customer.getDiscount() ) );

			OrderLine newOrderLine = new OrderLine(txId, null);
			newOrderLine.setOrderId(orderId);
			newOrderLine.setDistrictId(customerDistrictId);
			newOrderLine.setWarehouseId(supplyingWarehouse);
			newOrderLine.setLineNumber(i);
			newOrderLine.setItemId(item.getItemId());
			newOrderLine.setSupplyWarehouseId(supplyingWarehouse);
			newOrderLine.setQuantity((short) quantity);
			newOrderLine.setAmount(amount);
			newOrderLine.setDistInfo(getDistrictInfo(customerDistrictId).apply(originalStock));
			
			modificationCollection.addInsert(newOrderLine);
		}
	}
	
	private boolean allOrderLocal() {
		return supplyingWarehouseAndQuantityByItemId.values()
					.stream()
					.map(Pair::getKey)
					.allMatch( sw -> sw == terminalWarehouseId);
	}
}
