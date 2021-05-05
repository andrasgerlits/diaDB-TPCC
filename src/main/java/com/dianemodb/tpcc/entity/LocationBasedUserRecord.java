package com.dianemodb.tpcc.entity;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.UserBaseRecord;

public abstract class LocationBasedUserRecord extends UserBaseRecord {

	private String street1;
	private String street2;
	private String city;
	private String state;
	private String zip;
	
	@Deprecated
	protected LocationBasedUserRecord() {
		// required for serialization
	}

	public LocationBasedUserRecord(
			TransactionId txId, 
			RecordId recordId, 
			UserRecordTableId tableId
	) {
		super(txId, recordId, tableId);
	}

	public String getStreet1() {
		return street1;
	}

	public void setStreet1(String street1) {
		this.street1 = street1;
	}

	public String getStreet2() {
		return street2;
	}

	public void setStreet2(String street2) {
		this.street2 = street2;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		result = prime * result + ((street1 == null) ? 0 : street1.hashCode());
		result = prime * result + ((street2 == null) ? 0 : street2.hashCode());
		result = prime * result + ((zip == null) ? 0 : zip.hashCode());
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
		LocationBasedUserRecord other = (LocationBasedUserRecord) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (state == null) {
			if (other.state != null)
				return false;
		} else if (!state.equals(other.state))
			return false;
		if (street1 == null) {
			if (other.street1 != null)
				return false;
		} else if (!street1.equals(other.street1))
			return false;
		if (street2 == null) {
			if (other.street2 != null)
				return false;
		} else if (!street2.equals(other.street2))
			return false;
		if (zip == null) {
			if (other.zip != null)
				return false;
		} else if (!zip.equals(other.zip))
			return false;
		return true;
	}
}
