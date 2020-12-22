package com.dianemodb.tpcc.schema;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.tpcc.entity.AddressAndTaxUserBaseRecord;

public abstract class AddressAndTaxUserBaseTable<R extends AddressAndTaxUserBaseRecord> 
	extends LocationBasedUserRecordTable<R> 
{
	
	public final RecordColumn<R, String> nameColumn;

	public final RecordColumn<R, BigDecimal> taxColumn;
	public final RecordColumn<R, BigDecimal> ytdColumn;
	
	private final List<RecordColumn<R, ?>> columns;
	
	protected AddressAndTaxUserBaseTable(
			String name,
			UserRecordTableId tableId,
			String nameColumn, 
			String street1Column, 
			String street2Column, 
			String cityColumn, 
			String stateColumn, 
			String zipColumn,
			String taxColumn,
			String ytdColumn
	) {
		super(
			name, 
			tableId,			
			street1Column, 
			street2Column, 
			cityColumn, 
			stateColumn, 
			zipColumn
		);
		
		this.nameColumn = 
				new RecordColumn<>(
						new StringColumn(nameColumn, 10), 
						R::getName,
						R::setName
				);

		this.taxColumn = 
				new RecordColumn<>(
					new BigDecimalColumn(taxColumn, 4, 2), 
					R::getTax,
					R::setTax
				);
		 
		this.ytdColumn = 
				new RecordColumn<>(
					new BigDecimalColumn(ytdColumn, 12, 2), 
					R::getYtd,
					R::setYtd
				);
		
		this.columns = new LinkedList<>(super.columns());
		this.columns.add(this.nameColumn);
		this.columns.add(this.taxColumn);
		this.columns.add(this.ytdColumn);
	}

	@Override
	public List<RecordColumn<R, ?>> columns() {
		return this.columns;
	}
}
