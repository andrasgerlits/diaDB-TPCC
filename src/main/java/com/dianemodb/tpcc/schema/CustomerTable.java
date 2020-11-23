package com.dianemodb.tpcc.schema;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dianemodb.Topology;
import com.dianemodb.h2impl.NullRule;
import com.dianemodb.h2impl.RangeBasedDistributedIndex;
import com.dianemodb.id.RecordId;
import com.dianemodb.id.TransactionId;
import com.dianemodb.id.UserRecordTableId;
import com.dianemodb.metaschema.BigDecimalColumn;
import com.dianemodb.metaschema.ByteColumn;
import com.dianemodb.metaschema.IntColumn;
import com.dianemodb.metaschema.LongColumn;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.ShortColumn;
import com.dianemodb.metaschema.StringColumn;
import com.dianemodb.metaschema.TimestampColumn;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.distributed.ServerComputerIdNarrowingRule;
import com.dianemodb.tpcc.entity.Customer;

public class CustomerTable extends LocationBasedUserRecordTable<Customer> {

	public static final String TABLE_NAME = "customer";
	
	public static final String ID_COLUMN_NAME = "c_id";
	public static final String DISTRICT_ID_COLUMN_NAME = "c_d_id";
	public static final String WAREHOUSE_ID_COLUMN_NAME = "c_w_id";
	public static final String FIRST_NAME_COLUMN_NAME = "c_first";
	public static final String MIDDLE_NAME_COLUMN_NAME = "c_middle";
	public static final String LAST_NAME_COLUMN_NAME = "c_last";
	
	public static final String STREET_1_COLUMN_NAME = "c_street_1";
	public static final String STREET_2_COLUMN_NAME = "c_street_2";
	public static final String CITY_COLUMN_NAME = "c_city";
	public static final String STATE_COLUMN_NAME = "c_state";
	public static final String ZIP_COLUMN_NAME = "c_zip";
	
	public static final String PHONE_COLUMN_NAME = "c_phone";
	public static final String SINCE_COLUMN_NAME = "c_since";
	public static final String CREDIT_COLUMN_NAME = "c_credit";
	public static final String CREDIT_LIMIT_COLUMN_NAME = "c_credit_lim"; 
	public static final String DISCOUNT_COLUMN_NAME = "c_discount"; 
	public static final String BALANCE_COLUMN_NAME = "c_balance";
	public static final String YTD_PAYMENT_COLUMN_NAME = "c_ytd_payment";
	public static final String PAYMENT_CNT_COLUMN_NAME = "c_payment_cnt";
	public static final String DELIVERY_CNT_COLUMN_NAME = "c_delivery_cnt";
	public static final String DATA_COLUMN_NAME = "c_data";
	
	public static final RecordColumn<Customer, TransactionId> TX_ID_COLUMN = TX_ID();
	public static final RecordColumn<Customer, RecordId> RECORD_ID_COLUMN = RECORD_ID();

	public static final RecordColumn<Customer, Integer> ID_COLUMN = 
			new RecordColumn<>(
					new IntColumn(ID_COLUMN_NAME), 
					Customer::getPublicId,
					Customer::setPublicId
			);

	public static final RecordColumn<Customer, Byte> DISTRICT_ID_COLUMN =
			new RecordColumn<>(
					new ByteColumn(DISTRICT_ID_COLUMN_NAME), 
					Customer::getDistrictId,
					Customer::setDistrictId
			);

	public static final RecordColumn<Customer, Short> WAREHOUSE_ID_COLUMN =
			new RecordColumn<>(
					new ShortColumn(WAREHOUSE_ID_COLUMN_NAME), 
					Customer::getWarehouseId,
					Customer::setWarehouseId
			);
	
	public static final RecordColumn<Customer, String> FIRST_NAME_COLUMN = 
			new RecordColumn<>(
					new StringColumn(FIRST_NAME_COLUMN_NAME), 
					Customer::getFirstName,
					Customer::setFirstName
			);
	
	public static final RecordColumn<Customer, String> MIDDLE_NAME_COLUMN =
			new RecordColumn<>(
					new StringColumn(MIDDLE_NAME_COLUMN_NAME), 
					Customer::getMiddleName,
					Customer::setMiddleName
			);
	
	public static final RecordColumn<Customer, String> LAST_NAME_COLUMN = 
			new RecordColumn<>(
					new StringColumn(LAST_NAME_COLUMN_NAME), 
					Customer::getLastName,
					Customer::setLastName
			);
	
	public static final RecordColumn<Customer, String> PHONE_COLUMN =
			new RecordColumn<>(
					new StringColumn(PHONE_COLUMN_NAME), 
					Customer::getPhone,
					Customer::setPhone
			);
	
	public static final RecordColumn<Customer, Timestamp> SINCE_COLUMN =
			new RecordColumn<>(
					new TimestampColumn(SINCE_COLUMN_NAME), 
					Customer::getSince,
					Customer::setSince
			);
	
	public static final RecordColumn<Customer, String> CREDIT_COLUMN = 
			new RecordColumn<>(
					new StringColumn(CREDIT_COLUMN_NAME), 
					Customer::getCredit,
					Customer::setCredit
			);
	
	public static final RecordColumn<Customer, Long> CREDIT_LIMIT_COLUMN = 
			new RecordColumn<>(
					new LongColumn(CREDIT_LIMIT_COLUMN_NAME), 
					Customer::getCreditLimit,
					Customer::setCreditLimit
			);
	
	public static final RecordColumn<Customer, BigDecimal> DISCOUNT_COLUMN = 
			new RecordColumn<>(
					new BigDecimalColumn(DISCOUNT_COLUMN_NAME, 4, 2), 
					Customer::getDiscount,
					Customer::setDiscount
			);
	
	public static final RecordColumn<Customer, BigDecimal> BALANCE_COLUMN =
			new RecordColumn<>(
					new BigDecimalColumn(BALANCE_COLUMN_NAME, 12, 2), 
					Customer::getBalance,
					Customer::setBalance
			);
	
	public static final RecordColumn<Customer, BigDecimal> YTD_PAYMENT_COLUMN =
			new RecordColumn<>(
					new BigDecimalColumn(YTD_PAYMENT_COLUMN_NAME, 12, 2), 
					Customer::getYtdPayment,
					Customer::setYtdPayment
			);
	
	public static final RecordColumn<Customer, Short> PAYMENT_CNT_COLUMN =
			new RecordColumn<>(
					new ShortColumn(PAYMENT_CNT_COLUMN_NAME), 
					Customer::getPaymentCnt,
					Customer::setPaymentCnt
			);
	
	public static final RecordColumn<Customer, Short> DELIVERY_CNT_COLUMN =
			new RecordColumn<>(
					new ShortColumn(DELIVERY_CNT_COLUMN_NAME), 
					Customer::getDeliveryCnt,
					Customer::setDeliveryCnt
			);
	
	public static final RecordColumn<Customer, String> DATA_COLUMN =
			new RecordColumn<>(
					new StringColumn(DATA_COLUMN_NAME, 500), 
					Customer::getData,
					Customer::setData
			);


	public static final UserRecordTableId ID = new UserRecordTableId(CUSTOMER_TABLE_ID);

	private final Collection<DistributedIndex<Customer>> indices;
	private final List<RecordColumn<Customer, ?>> columns;
	private final RangeBasedDistributedIndex<Customer> compositeIndex;
	private final RangeBasedDistributedIndex<Customer> lastNameIndex;
	
	public CustomerTable(Topology servers) {
		super(
			TABLE_NAME, 
			ID, 
			STREET_1_COLUMN_NAME,
			STREET_2_COLUMN_NAME,
			CITY_COLUMN_NAME,
			STATE_COLUMN_NAME,
			ZIP_COLUMN_NAME
		);
		
		Map<RecordColumn<Customer,?>, ServerComputerIdNarrowingRule> basicRuleMap = 
				DistrictTable.getDistrictBasedRoundRobinRules(
						WAREHOUSE_ID_COLUMN, 
						DISTRICT_ID_COLUMN
				);
		
		Map<RecordColumn<Customer,?>, ServerComputerIdNarrowingRule> indexRuleMap =
				new HashMap<>(basicRuleMap);
		indexRuleMap.put(ID_COLUMN, NullRule.INSTANCE);
		
		compositeIndex = 
			new RangeBasedDistributedIndex<>(
				servers,
				this, 
				List.of(WAREHOUSE_ID_COLUMN, DISTRICT_ID_COLUMN, ID_COLUMN),
				indexRuleMap
			);
		
		Map<RecordColumn<Customer,?>, ServerComputerIdNarrowingRule> lastNameRuleMap =
				new HashMap<>(basicRuleMap);
		lastNameRuleMap.put(LAST_NAME_COLUMN, NullRule.INSTANCE);
		
		lastNameIndex = 
			new RangeBasedDistributedIndex<>(
					servers, 
					this, 
					List.of(WAREHOUSE_ID_COLUMN, DISTRICT_ID_COLUMN, LAST_NAME_COLUMN), 
					lastNameRuleMap
			);
		
		this.indices = List.of(compositeIndex, lastNameIndex);
		
		columns = new LinkedList<>(super.columns());
		columns.add(ID_COLUMN);
		columns.add(DISTRICT_ID_COLUMN);
		columns.add(WAREHOUSE_ID_COLUMN);
		columns.add(FIRST_NAME_COLUMN);
		columns.add(MIDDLE_NAME_COLUMN);
		columns.add(LAST_NAME_COLUMN);
		columns.add(PHONE_COLUMN);
		columns.add(SINCE_COLUMN);
		columns.add(CREDIT_COLUMN);
		columns.add(CREDIT_LIMIT_COLUMN);
		columns.add(DISCOUNT_COLUMN);
		columns.add(BALANCE_COLUMN);
		columns.add(YTD_PAYMENT_COLUMN);				
		columns.add(PAYMENT_CNT_COLUMN);
		columns.add(DELIVERY_CNT_COLUMN);
		columns.add(DATA_COLUMN);
	}

	@Override
	public Customer newInstance(TransactionId txId, RecordId recordId) {
		return new Customer(txId, recordId);
	}

	@Override
	public Class<Customer> entityClass() {
		return Customer.class;
	}

	public RangeBasedDistributedIndex<Customer> getCompositeIndex() {
		return compositeIndex;
	}

	@Override
	protected List<RecordColumn<Customer, ?>> columns() {
		return columns;
	}
	
	@Override
	protected DistributedIndex<Customer> getMaintainingComputerDecidingIndex() {
		return compositeIndex;
	}
	
	public RangeBasedDistributedIndex<Customer> getLastNameIndex() {
		return lastNameIndex;
	}

	@Override
	public Collection<DistributedIndex<Customer>> indices() {
		return indices;
	}
}
