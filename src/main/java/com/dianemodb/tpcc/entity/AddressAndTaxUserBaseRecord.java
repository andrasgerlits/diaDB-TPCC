package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;

public abstract class AddressAndTaxUserBaseRecord extends LocationBasedUserRecord {
	
	private String name;
	
	private BigDecimal tax;
	private BigDecimal ytd;
	
	protected AddressAndTaxUserBaseRecord(
			TransactionId txId, 
			RecordId recordId, 
			UserRecordTableId tableId
	) {
		super(txId, recordId, tableId);
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public BigDecimal getTax() {
		return tax;
	}

	public void setTax(BigDecimal tax) {
		this.tax = tax;
	}

	public BigDecimal getYtd() {
		return ytd;
	}

	public void setYtd(BigDecimal ytd) {
		this.ytd = ytd;
	}
}