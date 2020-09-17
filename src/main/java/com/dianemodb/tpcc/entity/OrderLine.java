package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class OrderLine extends UserBaseRecord {
	
	private int publicId;
	private short districtId;
	private short warehouseId;
	private short lineNumber;
	private short iId;
	private short supplyWarehouseId;
	private Timestamp deliveryDate;
	private short quantity;
	private BigDecimal amount;
	private String distInfo;
	
	public OrderLine(TransactionId txId, RecordId recordId) {
		super(txId, recordId, OrderLineTable.ID);
	}

	public int getPublicId() {
		return publicId;
	}

	public void setPublicId(int publicId) {
		this.publicId = publicId;
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

	public short getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(short lineNumber) {
		this.lineNumber = lineNumber;
	}

	public short getiId() {
		return iId;
	}

	public void setiId(short iId) {
		this.iId = iId;
	}

	public short getSupplyWarehouseId() {
		return supplyWarehouseId;
	}

	public void setSupplyWarehouseId(short supplyWarehouseId) {
		this.supplyWarehouseId = supplyWarehouseId;
	}

	public Timestamp getDeliveryDate() {
		return deliveryDate;
	}

	public void setDeliveryDate(Timestamp deliveryDate) {
		this.deliveryDate = deliveryDate;
	}

	public short getQuantity() {
		return quantity;
	}

	public void setQuantity(short quantity) {
		this.quantity = quantity;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getDistInfo() {
		return distInfo;
	}

	public void setDistInfo(String distInfo) {
		this.distInfo = distInfo;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((amount == null) ? 0 : amount.hashCode());
		result = prime * result + ((deliveryDate == null) ? 0 : deliveryDate.hashCode());
		result = prime * result + ((distInfo == null) ? 0 : distInfo.hashCode());
		result = prime * result + districtId;
		result = prime * result + iId;
		result = prime * result + lineNumber;
		result = prime * result + publicId;
		result = prime * result + quantity;
		result = prime * result + supplyWarehouseId;
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
		OrderLine other = (OrderLine) obj;
		if (amount == null) {
			if (other.amount != null)
				return false;
		} else if (!amount.equals(other.amount))
			return false;
		if (deliveryDate == null) {
			if (other.deliveryDate != null)
				return false;
		} else if (!deliveryDate.equals(other.deliveryDate))
			return false;
		if (distInfo == null) {
			if (other.distInfo != null)
				return false;
		} else if (!distInfo.equals(other.distInfo))
			return false;
		if (districtId != other.districtId)
			return false;
		if (iId != other.iId)
			return false;
		if (lineNumber != other.lineNumber)
			return false;
		if (publicId != other.publicId)
			return false;
		if (quantity != other.quantity)
			return false;
		if (supplyWarehouseId != other.supplyWarehouseId)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "OrderLine [publicId=" + publicId + ", districtId=" + districtId + ", warehouseId=" + warehouseId
				+ ", lineNumber=" + lineNumber + ", iId=" + iId + ", supplyWarehouseId=" + supplyWarehouseId
				+ ", deliveryDate=" + deliveryDate + ", quantity=" + quantity + ", amount=" + amount + ", distInfo="
				+ distInfo + ", txId=" + txId + ", recordId=" + recordId + "]";
	}
}
