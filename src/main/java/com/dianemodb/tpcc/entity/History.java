package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.HistoryTable;

public class History extends UserBaseRecord {

	private int customerId;
	private byte customerDistrictId;
	private short customerWarehouseId;
	private Byte districtId;
	private short warehouseId;
	private long date;
	private String amount;
	private String data;
	
	@Deprecated
	@SuppressWarnings({ "unused"})
	private History() {
		// required for serialization
	}

	public History(TransactionId txId, RecordId recordId) {
		super(txId, recordId, HistoryTable.ID);
	}

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public byte getCustomerDistrictId() {
		return customerDistrictId;
	}

	public void setCustomerDistrictId(byte customerDistrictId) {
		this.customerDistrictId = customerDistrictId;
	}

	public short getCustomerWarehouseId() {
		return customerWarehouseId;
	}

	public void setCustomerWarehouseId(short customerWarehouseId) {
		this.customerWarehouseId = customerWarehouseId;
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

	public Timestamp getDate() {
		return new Timestamp(date);
	}

	public void setDate(Timestamp date) {
		this.date = date.getTime();
	}

	public BigDecimal getAmount() {
		return newBigDecimal(amount);
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount.toPlainString();
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((amount == null) ? 0 : amount.hashCode());
		result = prime * result + customerDistrictId;
		result = prime * result + customerId;
		result = prime * result + customerWarehouseId;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + (int) (date ^ (date >>> 32));
		result = prime * result + districtId;
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
		History other = (History) obj;
		if (amount == null) {
			if (other.amount != null)
				return false;
		} else if (!amount.equals(other.amount))
			return false;
		if (customerDistrictId != other.customerDistrictId)
			return false;
		if (customerId != other.customerId)
			return false;
		if (customerWarehouseId != other.customerWarehouseId)
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (date != other.date)
			return false;
		if (districtId != other.districtId)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "History [customerId=" + customerId + ", customerDistrictId=" + customerDistrictId
				+ ", customerWarehouseId=" + customerWarehouseId + ", districtId=" + districtId + ", warehouseId="
				+ warehouseId + ", date=" + getDate() + ", amount=" + amount + ", data=" + data + ", txId=" + txId
				+ ", recordId=" + recordId + "]";
	}

	
}
