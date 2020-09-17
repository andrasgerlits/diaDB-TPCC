package com.dianemodb.tpcc.entity;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.tpcc.schema.WarehouseTable;

public class Warehouse extends AddressAndTaxUserBaseRecord {
	
	private short publicId;
	
	public Warehouse(TransactionId txId, RecordId recordId) {
		super(txId, recordId, WarehouseTable.ID);
	}
	
	public short getPublicId() {
		return publicId;
	}

	public void setPublicId(short publicId) {
		this.publicId = publicId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + publicId;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Warehouse other = (Warehouse) obj;
		if (publicId != other.publicId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Warehouse [publicId=" + publicId + ", getName()=" + getName() + ", getStreet1()=" + getStreet1()
				+ ", getStreet2()=" + getStreet2() + ", getCity()=" + getCity() + ", getZip()=" + getZip()
				+ ", getTax()=" + getTax() + ", getYtd()=" + getYtd() + ", getState()=" + getState() + ", getTableId()="
				+ getTableId() + ", txId()=" + txId() + ", recordId()=" + recordId() + "]";
	}
}
