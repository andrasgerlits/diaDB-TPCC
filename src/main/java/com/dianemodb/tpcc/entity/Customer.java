package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.tpcc.schema.CustomerTable;

public class Customer extends LocationBasedUserRecord {
	
	private int publicId;
	private byte districtId;
	private short warehouseId;
	private String firstName;
	private String middleName;
	private String lastName;
	private String phone;
	private long since;
	private String credit;
	private long creditLimit;
	private String discount;
	private String balance;
	private String ytdPayment;
	private short paymentCnt;
	private short deliveryCnt;
	private String data;
	
	@Deprecated
	@SuppressWarnings({ "unused"})
	private Customer() {
		// required for serialization
	}

	public Customer(TransactionId txId, RecordId recordId) {
		super(txId, recordId, CustomerTable.ID);
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

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Timestamp getSince() {
		return new Timestamp(since);
	}

	public void setSince(Timestamp since) {
		this.since = since.getTime();
	}

	public String getCredit() {
		return credit;
	}

	public void setCredit(String credit) {
		this.credit = credit;
	}

	public long getCreditLimit() {
		return creditLimit;
	}

	public void setCreditLimit(long creditLimit) {
		this.creditLimit = creditLimit;
	}

	public BigDecimal getDiscount() {
		return newBigDecimal(discount);
	}

	public void setDiscount(BigDecimal discount) {
		this.discount = discount.toPlainString();
	}

	public BigDecimal getBalance() {
		return newBigDecimal(balance);
	}

	public void setBalance(BigDecimal balance) {
		this.balance = balance.toPlainString();
	}

	public BigDecimal getYtdPayment() {
		return newBigDecimal(ytdPayment);
	}

	public void setYtdPayment(BigDecimal ytdPayment) {
		this.ytdPayment = ytdPayment == null? null : ytdPayment.toPlainString();
	}

	public short getPaymentCnt() {
		return paymentCnt;
	}

	public void setPaymentCnt(short paymentCnt) {
		this.paymentCnt = paymentCnt;
	}

	public short getDeliveryCnt() {
		return deliveryCnt;
	}

	public void setDeliveryCnt(short deliveryCnt) {
		this.deliveryCnt = deliveryCnt;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int getPublicId() {
		return publicId;
	}

	public void setPublicId(int publicId) {
		this.publicId = publicId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((balance == null) ? 0 : balance.hashCode());
		result = prime * result + ((credit == null) ? 0 : credit.hashCode());
		result = prime * result + (int) (creditLimit ^ (creditLimit >>> 32));
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + deliveryCnt;
		result = prime * result + ((discount == null) ? 0 : discount.hashCode());
		result = prime * result + districtId;
		result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		result = prime * result + ((middleName == null) ? 0 : middleName.hashCode());
		result = prime * result + paymentCnt;
		result = prime * result + ((phone == null) ? 0 : phone.hashCode());
		result = prime * result + publicId;
		result = prime * result + (int) (since ^ (since >>> 32));
		result = prime * result + warehouseId;
		result = prime * result + ((ytdPayment == null) ? 0 : ytdPayment.hashCode());
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
		Customer other = (Customer) obj;
		if (balance == null) {
			if (other.balance != null)
				return false;
		} else if (!balance.equals(other.balance))
			return false;
		if (credit == null) {
			if (other.credit != null)
				return false;
		} else if (!credit.equals(other.credit))
			return false;
		if (creditLimit != other.creditLimit)
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (deliveryCnt != other.deliveryCnt)
			return false;
		if (discount == null) {
			if (other.discount != null)
				return false;
		} else if (!discount.equals(other.discount))
			return false;
		if (districtId != other.districtId)
			return false;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		if (middleName == null) {
			if (other.middleName != null)
				return false;
		} else if (!middleName.equals(other.middleName))
			return false;
		if (paymentCnt != other.paymentCnt)
			return false;
		if (phone == null) {
			if (other.phone != null)
				return false;
		} else if (!phone.equals(other.phone))
			return false;
		if (publicId != other.publicId)
			return false;
		if (since != other.since)
			return false;
		if (warehouseId != other.warehouseId)
			return false;
		if (ytdPayment == null) {
			if (other.ytdPayment != null)
				return false;
		} else if (!ytdPayment.equals(other.ytdPayment))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Customer [publicId=" + publicId + ", districtId=" + districtId + ", warehouseId=" + warehouseId
				+ ", firstName=" + firstName + ", middleName=" + middleName + ", lastName=" + lastName + ", phone="
				+ phone + ", since=" + since + ", credit=" + credit + ", creditLimit=" + creditLimit + ", discount="
				+ discount + ", balance=" + balance + ", ytdPayment=" + ytdPayment + ", paymentCnt=" + paymentCnt
				+ ", deliveryCnt=" + deliveryCnt + ", data=" + data + "]";
	}

}
