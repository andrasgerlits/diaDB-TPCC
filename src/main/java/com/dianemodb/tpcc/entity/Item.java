package com.dianemodb.tpcc.entity;

import java.math.BigDecimal;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.ItemTable;

public class Item extends UserBaseRecord {

	private int publicId;
	private int im;
	private String name;
	private BigDecimal price;
	private String data;
	
	public Item(TransactionId txId, RecordId recordId) {
		super(txId, recordId, ItemTable.ID);
	}

	public int getPublicId() {
		return publicId;
	}

	public void setPublicId(int publicId) {
		this.publicId = publicId;
	}

	public int getIm() {
		return im;
	}

	public void setIm(int im) {
		this.im = im;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
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
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + im;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((price == null) ? 0 : price.hashCode());
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
		Item other = (Item) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (im != other.im)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		if (publicId != other.publicId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Item [publicId=" + publicId + ", im=" + im + ", name=" + name + ", price=" + price + ", data=" + data
				+ ", txId=" + txId + ", recordId=" + recordId + "]";
	}
}
