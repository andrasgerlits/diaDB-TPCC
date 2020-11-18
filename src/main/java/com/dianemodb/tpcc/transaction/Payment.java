package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.dianemodb.ModificationCollection;
import com.dianemodb.RecordWithVersion;
import com.dianemodb.ServerComputerId;
import com.dianemodb.message.Envelope;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.tpcc.entity.Customer;
import com.dianemodb.tpcc.entity.District;
import com.dianemodb.tpcc.entity.Warehouse;
import com.dianemodb.tpcc.query.CustomerSelectionStrategy;
import com.dianemodb.tpcc.query.FindDistrictByIdAndWarehouse;
import com.dianemodb.tpcc.query.FindWarehouseDetailsById;

public class Payment extends TpccTestProcess {
	
	private final BigDecimal amount;
	
	/*
	 * The customer must be selected for the warehouse/district, but might not
	 */
	protected final short homeWarehouseId;
	protected final byte homeDistrictId;
	protected final CustomerSelectionStrategy customerSelectionStrategy;
	protected final boolean isHomePayment;
	
	public Payment(
			Random random,
			ServerComputerId txComputer,
			SQLServerApplication application,
			short warehouseId,
			byte districtId
	) {
		super(random, application, txComputer, 5000);
		
		this.customerSelectionStrategy = randomStrategy(random, warehouseId, districtId);
		isHomePayment = random.nextInt(85) + 1 <= 85;
		amount = new BigDecimal(random.nextInt(4999) + 1);
		
		this.homeWarehouseId = warehouseId;
		this.homeDistrictId = districtId;
	}

	@Override
	protected Result startTx() {
		Envelope customerQuery = customerSelectionStrategy.customerQuery(this);
		
		if(isHomePayment) {
			List<Envelope> envelopeList = new LinkedList<>(); 
			envelopeList.add(customerQuery);
			envelopeList.addAll(warehouseDistrictQueries(homeWarehouseId, homeDistrictId));
			return of(envelopeList, this::selectCustomerUpdateRecords);
		}
		else {
			return of(customerQuery, this::queryDistrictWarehouse);
		}
	}

	@SuppressWarnings("unchecked")
	private Result queryDistrictWarehouse(Object results) {
		RecordWithVersion<Customer> customerRecord = (RecordWithVersion<Customer>) results;
		
		short warehouseId = customerRecord.getRecord().getWarehouseId();			
		byte districtId = customerRecord.getRecord().getDistrictId();
				
		List<Envelope> envelopeList = warehouseDistrictQueries(warehouseId, districtId);
		
		return of(envelopeList, l -> this.updateRecords(l, customerRecord));
	}

	private List<Envelope> warehouseDistrictQueries(short warehouseId, byte districtId) {
		Envelope queryWarehouseEnvelope = 
				query(
					FindWarehouseDetailsById.ID, 
					List.of(warehouseId)
				);
		
		Envelope queryDistrictEnvelope =
				query(
					FindDistrictByIdAndWarehouse.ID, 
					List.of(districtId)
				);
		
		List<Envelope> envelopeList = 
				List.of(
					queryWarehouseEnvelope, 
					queryDistrictEnvelope 
				);
		return envelopeList;
	}
	
	private Result selectCustomerUpdateRecords(List<Object> results) {
		Iterator<Object> resultIter = results.iterator();
		
		RecordWithVersion<Customer> customerWithVersion = customerSelectionStrategy.getCustomerFromResult(resultIter.next());
		RecordWithVersion<Warehouse> warehouseWithVersion = singleFromResultList(resultIter.next());
		RecordWithVersion<District> districtWithVersion = singleFromResultList(resultIter.next());
		
		List<Object> resultList = List.of(warehouseWithVersion, districtWithVersion);
		
		return updateRecords(resultList, customerWithVersion);
	}
	
	private Result updateRecords(List<Object> results, RecordWithVersion<Customer> customerWithVersion) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());

		ModificationCollection modificationCollection = new ModificationCollection();

		Iterator<Object> resultIter = results.iterator();
		RecordWithVersion<Warehouse> warehouseWithVersion = singleFromResultList(resultIter.next());
		RecordWithVersion<District> districtWithVersion = singleFromResultList(resultIter.next());

		Warehouse updatedWarehouse = warehouseWithVersion.getRecord().shallowClone(application, txId);
		updatedWarehouse.setYtd(updatedWarehouse.getYtd().add(amount));
		modificationCollection.addUpdate(warehouseWithVersion, updatedWarehouse);
		
		District updatedDistrict = districtWithVersion.getRecord().shallowClone(application, txId);
		updatedDistrict.setYtd(updatedDistrict.getYtd().add(amount));
		modificationCollection.addUpdate(districtWithVersion, updatedDistrict);
		
		Customer customer = customerWithVersion.getRecord();

		String updatedData = 
				String.format(
						"| %d %d %d %d %d $%f %s %s", 
						customer.getPublicId(), 
						customer.getDistrictId(), 
						customer.getWarehouseId(), 
						districtWithVersion.getRecord().getId(), 
						warehouseWithVersion.getRecord().getPublicId(), 
						amount.floatValue(), 
						timestamp.toString(), 
						customer.getData()
				);


		Customer updatedCustomer = customer.shallowClone(application, txId);
		updatedCustomer.setBalance(updatedCustomer.getBalance().add(amount));
		updatedCustomer.setData(updatedData);
		
		modificationCollection.addUpdate(customerWithVersion, updatedCustomer);
		
		return of(List.of(modifyEvent(modificationCollection)), this::commit);
	}
}
