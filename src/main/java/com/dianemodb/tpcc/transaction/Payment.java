package com.dianemodb.tpcc.transaction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

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
	protected final short warehouseId;
	protected final short districtId;
	protected final CustomerSelectionStrategy customerSelectionStrategy;
	
	public Payment(
			ServerComputerId txComputer,
			SQLServerApplication application,
			BigDecimal amount,
			short warehouseId,
			short districtId,
			CustomerSelectionStrategy selectionStrategy
	) {
		super(application, txComputer);
		
		this.amount = amount;
		this.warehouseId = warehouseId;
		this.districtId = districtId;
		this.customerSelectionStrategy = selectionStrategy;
	}

	@Override
	protected Result startTx() {
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
		
		Envelope customerQuery = customerSelectionStrategy.customerQuery(this);
		
		return of(
				List.of(
					queryWarehouseEnvelope, 
					queryDistrictEnvelope, 
					customerQuery
				), 
				this::updateRecords
			);
	}
	
	private Result updateRecords(List<Object> results) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());

		Iterator<Object> resultIter = results.iterator();
		RecordWithVersion<Warehouse> warehouseWithVersion = singleFromResultList(resultIter.next());
		RecordWithVersion<District> districtWithVersion = singleFromResultList(resultIter.next());
		RecordWithVersion<Customer> customerWithVersion = customerSelectionStrategy.getCustomerFromResult(resultIter.next());

		ModificationCollection modificationCollection = new ModificationCollection();

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
						districtWithVersion.getRecord().getPublicId(), 
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
