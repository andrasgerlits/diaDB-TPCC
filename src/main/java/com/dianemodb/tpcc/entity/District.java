package com.dianemodb.tpcc.entity;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.tpcc.schema.DistrictTable;

public class District extends AddressAndTaxUserBaseRecord {
	
	private short warehouseId;
	private int nextOid;
	private byte publicId;
	
	public District(TransactionId txId, RecordId recordId) {
		super(txId, recordId, DistrictTable.ID);
	}

	public short getWarehouseId() {
		return warehouseId;
	}

	public void setWarehouseId(short warehouseId) {
		this.warehouseId = warehouseId;
	}

	public int getNextOid() {
		return nextOid;
	}

	public void setNextOid(int nextOid) {
		this.nextOid = nextOid;
	}

	public byte getPublicId() {
		return publicId;
	}

	public void setPublicId(byte publicId) {
		this.publicId = publicId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + nextOid;
		result = prime * result + publicId;
		result = prime * result + warehouseId;
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
		District other = (District) obj;
		if (nextOid != other.nextOid)
			return false;
		if (publicId != other.publicId)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "District [publicId=" + publicId + ", warehouseId=" + warehouseId + ", getName()=" + getName()
				+ ", getStreet1()=" + getStreet1() + ", getStreet2()=" + getStreet2() + ", getCity()=" + getCity()
				+ ", getZip()=" + getZip() + ", getTax()=" + getTax() + ", getYtd()=" + getYtd() + ", getState()="
				+ getState() + ", txId()=" + txId() + ", recordId()=" + recordId() + "]";
	}

}
