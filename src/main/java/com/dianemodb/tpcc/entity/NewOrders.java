package com.dianemodb.tpcc.entity;

import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.NewOrdersTable;

public class NewOrders extends UserBaseRecord {
	
	private int orderId;
	private byte districtId;
	private short warehouseId;
	private int customerId;
	private long entryTime;
	private short carrierId;
	private short line;
	private short allLocal;
	
	@Deprecated
	@SuppressWarnings({ "unused"})
	private NewOrders() {
		// required for serialization
	}

	public NewOrders(TransactionId txId, RecordId recordId) {
		super(txId, recordId, NewOrdersTable.ID);
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int publicId) {
		this.orderId = publicId;
	}

	public byte getDistrictId() {
		return districtId;
	}

	public void setDistrictId(byte districtId) {
		this.districtId = districtId;
	}

	public short getWarehouseId() {
		return warehouseId;
	}

	public void setWarehouseId(short warehouseId) {
		this.warehouseId = warehouseId;
	}

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public Timestamp getEntryTime() {
		return new Timestamp(entryTime);
	}

	public void setEntryTime(Timestamp entryTime) {
		this.entryTime = entryTime.getTime();
	}

	public short getCarrierId() {
		return carrierId;
	}

	public void setCarrierId(short carrierId) {
		this.carrierId = carrierId;
	}

	public short getLine() {
		return line;
	}

	public void setLine(short line) {
		this.line = line;
	}

	public short getAllLocal() {
		return allLocal;
	}

	public void setAllLocal(short allLocal) {
		this.allLocal = allLocal;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + allLocal;
		result = prime * result + carrierId;
		result = prime * result + customerId;
		result = prime * result + districtId;
		result = prime * result + (int) (entryTime ^ (entryTime >>> 32));
		result = prime * result + line;
		result = prime * result + orderId;
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
		NewOrders other = (NewOrders) obj;
		if (allLocal != other.allLocal)
			return false;
		if (carrierId != other.carrierId)
			return false;
		if (customerId != other.customerId)
			return false;
		if (districtId != other.districtId)
			return false;
		if (entryTime != other.entryTime)
			return false;
		if (line != other.line)
			return false;
		if (orderId != other.orderId)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NewOrders [orderId=" + orderId + ", districtId=" + districtId + ", warehouseId=" + warehouseId
				+ ", customerId=" + customerId + ", entryTime=" + entryTime + ", carrierId=" + carrierId + ", line="
				+ line + ", allLocal=" + allLocal + ", txId=" + txId + ", recordId=" + recordId + "]";
	}
}
