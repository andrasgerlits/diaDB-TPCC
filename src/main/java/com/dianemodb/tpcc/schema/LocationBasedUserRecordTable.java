package com.dianemodb.tpcc.schema;

import java.util.LinkedList;
import java.util.List;

import com.dianemodb.Topology;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.tpcc.entity.LocationBasedUserRecord;

public abstract class LocationBasedUserRecordTable<R extends LocationBasedUserRecord> extends WarehouseBasedTable<R> {
	
	protected final RecordColumn<R, String> street1Column;
	protected final RecordColumn<R, String> street2Column;
	protected final RecordColumn<R, String> cityColumn;
	protected final RecordColumn<R, String> stateColumn;
	protected final RecordColumn<R, String> zipColumn;

	private final List<RecordColumn<R, ?>> columns;

	public LocationBasedUserRecordTable(
			String name,
			UserRecordTableId tableId,
			String street1Column, 
			String street2Column, 
			String cityColumn, 
			String stateColumn, 
			String zipColumn,
			Topology topology
	) {
		super(tableId, name, topology);
		
		this.street1Column = new RecordColumn<>(new StringColumn(street1Column, 20), R::getStreet1, R::setStreet1);
		this.street2Column = new RecordColumn<>(new StringColumn(street2Column, 20), R::getStreet2, R::setStreet2);
		this.cityColumn = new RecordColumn<>(new StringColumn(cityColumn,20), R::getCity, R::setCity);	
		this.stateColumn = new RecordColumn<>(new StringColumn(stateColumn, 2), R::getState, R::setState);		
		this.zipColumn = new RecordColumn<>(new StringColumn(zipColumn, 9), R::getZip, R::setZip);
		
		this.columns = new LinkedList<>(super.columns());
		
		List<RecordColumn<R, ?>> ownColumns =
			List.of(
				this.street1Column, 
				this.street2Column, 
				this.cityColumn, 
				this.stateColumn, 
				this.zipColumn
			);
		
		this.columns.addAll(ownColumns);
	}

	@Override
	protected List<RecordColumn<R, ?>> columns() {
		return columns;
	}	
}
