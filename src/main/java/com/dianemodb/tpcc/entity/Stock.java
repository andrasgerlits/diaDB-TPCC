package com.dianemodb.tpcc.entity;

import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.metaschema.UserBaseRecord;
import com.dianemodb.tpcc.schema.StockTable;

/*
 create table stock (
s_i_id int not null, 
s_w_id smallint not null, 
s_quantity smallint, 
s_dist_01 char(24), 
s_dist_02 char(24),
s_dist_03 char(24),
s_dist_04 char(24), 
s_dist_05 char(24), 
s_dist_06 char(24), 
s_dist_07 char(24), 
s_dist_08 char(24), 
s_dist_09 char(24), 
s_dist_10 char(24), 
s_ytd decimal(8,0), 
s_order_cnt smallint, 
s_remote_cnt smallint,
s_data varchar(50),
PRIMARY KEY(s_w_id, s_i_id) ) Engine=InnoDB 
 */
public class Stock extends UserBaseRecord {
	
	private int itemId;
	private short warehousId;
	private short quantity;
	private String dist1;
	private String dist2;
	private String dist3;
	private String dist4;
	private String dist5;
	private String dist6;
	private String dist7;
	private String dist8;
	private String dist9;
	private String dist10;
	private long ytd;
	private short orderCnt;
	private short remoteCnt;
	private String data;
	
	@Deprecated
	@SuppressWarnings({ "unused"})
	private Stock() {
		// required for serialization
	}
	
	public Stock(TransactionId txId, RecordId recordId) {
		super(txId, recordId, StockTable.ID);
	}

	public int getItemId() {
		return itemId;
	}

	public void setItemId(int itemId) {
		this.itemId = itemId;
	}

	public short getWarehouseId() {
		return warehousId;
	}

	public void setWarehouseId(short warehousId) {
		this.warehousId = warehousId;
	}

	public short getQuantity() {
		return quantity;
	}

	public void setQuantity(short quantity) {
		this.quantity = quantity;
	}

	public String getDist1() {
		return dist1;
	}

	public void setDist1(String dist1) {
		this.dist1 = dist1;
	}

	public String getDist2() {
		return dist2;
	}

	public void setDist2(String dist2) {
		this.dist2 = dist2;
	}

	public String getDist3() {
		return dist3;
	}

	public void setDist3(String dist3) {
		this.dist3 = dist3;
	}

	public String getDist4() {
		return dist4;
	}

	public void setDist4(String dist4) {
		this.dist4 = dist4;
	}

	public String getDist5() {
		return dist5;
	}

	public void setDist5(String dist5) {
		this.dist5 = dist5;
	}

	public String getDist6() {
		return dist6;
	}

	public void setDist6(String dist6) {
		this.dist6 = dist6;
	}

	public String getDist7() {
		return dist7;
	}

	public void setDist7(String dist7) {
		this.dist7 = dist7;
	}

	public String getDist8() {
		return dist8;
	}

	public void setDist8(String dist8) {
		this.dist8 = dist8;
	}

	public String getDist9() {
		return dist9;
	}

	public void setDist9(String dist9) {
		this.dist9 = dist9;
	}

	public String getDist10() {
		return dist10;
	}

	public void setDist10(String dist10) {
		this.dist10 = dist10;
	}

	public Long getYtd() {
		return ytd;
	}

	public void setYtd(Long ytd) {
		this.ytd = ytd;
	}

	public short getOrderCnt() {
		return orderCnt;
	}

	public void setOrderCnt(short orderCnt) {
		this.orderCnt = orderCnt;
	}

	public short getRemoteCnt() {
		return remoteCnt;
	}

	public void setRemoteCnt(short remoteCnt) {
		this.remoteCnt = remoteCnt;
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
		result = prime * result + ((dist1 == null) ? 0 : dist1.hashCode());
		result = prime * result + ((dist10 == null) ? 0 : dist10.hashCode());
		result = prime * result + ((dist2 == null) ? 0 : dist2.hashCode());
		result = prime * result + ((dist3 == null) ? 0 : dist3.hashCode());
		result = prime * result + ((dist4 == null) ? 0 : dist4.hashCode());
		result = prime * result + ((dist5 == null) ? 0 : dist5.hashCode());
		result = prime * result + ((dist6 == null) ? 0 : dist6.hashCode());
		result = prime * result + ((dist7 == null) ? 0 : dist7.hashCode());
		result = prime * result + ((dist8 == null) ? 0 : dist8.hashCode());
		result = prime * result + ((dist9 == null) ? 0 : dist9.hashCode());
		result = prime * result + orderCnt;
		result = prime * result + itemId;
		result = prime * result + quantity;
		result = prime * result + remoteCnt;
		result = prime * result + warehousId;
		result = prime * result + (int) (ytd ^ (ytd >>> 32));
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
		Stock other = (Stock) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (dist1 == null) {
			if (other.dist1 != null)
				return false;
		} else if (!dist1.equals(other.dist1))
			return false;
		if (dist10 == null) {
			if (other.dist10 != null)
				return false;
		} else if (!dist10.equals(other.dist10))
			return false;
		if (dist2 == null) {
			if (other.dist2 != null)
				return false;
		} else if (!dist2.equals(other.dist2))
			return false;
		if (dist3 == null) {
			if (other.dist3 != null)
				return false;
		} else if (!dist3.equals(other.dist3))
			return false;
		if (dist4 == null) {
			if (other.dist4 != null)
				return false;
		} else if (!dist4.equals(other.dist4))
			return false;
		if (dist5 == null) {
			if (other.dist5 != null)
				return false;
		} else if (!dist5.equals(other.dist5))
			return false;
		if (dist6 == null) {
			if (other.dist6 != null)
				return false;
		} else if (!dist6.equals(other.dist6))
			return false;
		if (dist7 == null) {
			if (other.dist7 != null)
				return false;
		} else if (!dist7.equals(other.dist7))
			return false;
		if (dist8 == null) {
			if (other.dist8 != null)
				return false;
		} else if (!dist8.equals(other.dist8))
			return false;
		if (dist9 == null) {
			if (other.dist9 != null)
				return false;
		} else if (!dist9.equals(other.dist9))
			return false;
		if (orderCnt != other.orderCnt)
			return false;
		if (itemId != other.itemId)
			return false;
		if (quantity != other.quantity)
			return false;
		if (remoteCnt != other.remoteCnt)
			return false;
		if (warehousId != other.warehousId)
			return false;
		if (ytd != other.ytd)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Stock [itemId=" + itemId + ", warehousId=" + warehousId + ", quantity=" + quantity + ", dist1="
				+ dist1 + ", dist2=" + dist2 + ", dist3=" + dist3 + ", dist4=" + dist4 + ", dist5=" + dist5 + ", dist6="
				+ dist6 + ", dist7=" + dist7 + ", dist8=" + dist8 + ", dist9=" + dist9 + ", dist10=" + dist10 + ", ytd="
				+ ytd + ", orderCnt=" + orderCnt + ", remoteCnt=" + remoteCnt + ", data=" + data + ", txId=" + txId
				+ ", recordId=" + recordId + "]";
	}
}
