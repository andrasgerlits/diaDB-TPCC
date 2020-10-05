package com.dianemodb.tpcc.entity;

import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.OrdersTable;

public class Orders extends UserBaseRecord {
	
	private int orderId;
	private short districtId;
	private short warehouseId;
	private int customerId;
	private Timestamp entryDate;
	private short carrierId;
	private short orderLineCount;
	private short allLocal;

	public Orders(TransactionId txId, RecordId recordId) {
		super(txId, recordId, OrdersTable.ID);
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int publicId) {
		this.orderId = publicId;
	}

	public short getDistrictId() {
		return districtId;
	}

	public void setDistrictId(short districtId) {
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

	public Timestamp getEntryDate() {
		return entryDate;
	}

	public void setEntryDate(Timestamp entryDate) {
		this.entryDate = entryDate;
	}

	public short getCarrierId() {
		return carrierId;
	}

	public void setCarrierId(short carrierId) {
		this.carrierId = carrierId;
	}

	public short getOrderLineCount() {
		return orderLineCount;
	}

	public void setOrderLineCount(short orderLineCount) {
		this.orderLineCount = orderLineCount;
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
		result = prime * result + ((entryDate == null) ? 0 : entryDate.hashCode());
		result = prime * result + orderLineCount;
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
		Orders other = (Orders) obj;
		if (allLocal != other.allLocal)
			return false;
		if (carrierId != other.carrierId)
			return false;
		if (customerId != other.customerId)
			return false;
		if (districtId != other.districtId)
			return false;
		if (entryDate == null) {
			if (other.entryDate != null)
				return false;
		} else if (!entryDate.equals(other.entryDate))
			return false;
		if (orderLineCount != other.orderLineCount)
			return false;
		if (orderId != other.orderId)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Orders [publicId=" + orderId + ", districtId=" + districtId + ", warehouseId=" + warehouseId
				+ ", customerId=" + customerId + ", entryDate=" + entryDate + ", carrierId=" + carrierId
				+ ", orderLineCount=" + orderLineCount + ", allLocal=" + allLocal + ", txId=" + txId + ", recordId="
				+ recordId + "]";
	}
}
