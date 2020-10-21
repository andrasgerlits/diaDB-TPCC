package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.OrderLineTable;

public class OrderLine extends UserBaseRecord {
	
	private int orderId;
	private byte districtId;
	private short warehouseId;
	private short lineNumber;
	private short itemId;
	private short supplyWarehouseId;
	private long deliveryDate;
	private short quantity;
	private String amount;
	private String distInfo;
	
	@Deprecated
	@SuppressWarnings({ "unused"})
	private OrderLine() {
		// required for serialization
	}
	
	public OrderLine(TransactionId txId, RecordId recordId) {
		super(txId, recordId, OrderLineTable.ID);
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int orderId) {
		this.orderId = orderId;
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

	public short getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(short lineNumber) {
		this.lineNumber = lineNumber;
	}

	public short getItemId() {
		return itemId;
	}

	public void setItemId(short iId) {
		this.itemId = iId;
	}

	public short getSupplyWarehouseId() {
		return supplyWarehouseId;
	}

	public void setSupplyWarehouseId(short supplyWarehouseId) {
		this.supplyWarehouseId = supplyWarehouseId;
	}

	public Timestamp getDeliveryDate() {
		return new Timestamp(deliveryDate);
	}

	public void setDeliveryDate(Timestamp deliveryDate) {
		this.deliveryDate = deliveryDate.getTime();
	}

	public short getQuantity() {
		return quantity;
	}

	public void setQuantity(short quantity) {
		this.quantity = quantity;
	}

	public BigDecimal getAmount() {
		return newBigDecimal(amount);
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount.toPlainString();
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
		result = prime * result + (int) (deliveryDate ^ (deliveryDate >>> 32));
		result = prime * result + ((distInfo == null) ? 0 : distInfo.hashCode());
		result = prime * result + districtId;
		result = prime * result + itemId;
		result = prime * result + lineNumber;
		result = prime * result + orderId;
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
		if (deliveryDate != other.deliveryDate)
			return false;
		if (distInfo == null) {
			if (other.distInfo != null)
				return false;
		} else if (!distInfo.equals(other.distInfo))
			return false;
		if (districtId != other.districtId)
			return false;
		if (itemId != other.itemId)
			return false;
		if (lineNumber != other.lineNumber)
			return false;
		if (orderId != other.orderId)
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
		return "OrderLine [orderId=" + orderId + ", districtId=" + districtId + ", warehouseId=" + warehouseId
				+ ", lineNumber=" + lineNumber + ", itemId=" + itemId + ", supplyWarehouseId=" + supplyWarehouseId
				+ ", deliveryDate=" + deliveryDate + ", quantity=" + quantity + ", amount=" + amount + ", distInfo="
				+ distInfo + ", txId=" + txId + ", recordId=" + recordId + "]";
	}
}
