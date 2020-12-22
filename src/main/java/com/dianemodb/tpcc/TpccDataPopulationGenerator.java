package com.dianemodb.tpcc;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import com.dianemodb.ServerComputerId;
import com.dianemodb.StorageConnection;
import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.computertest.framework.TestComputer;
import com.dianemodb.id.RecordId;
import com.dianemodb.integration.sqlwrapper.BenchmarkingH2ConnectionWrapper;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLHelper;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.DistributedIndex;
import com.dianemodb.metaschema.index.IndexRecord;
import com.dianemodb.metaschema.index.IndexTable;
import com.dianemodb.metaschema.schema.ServerTable;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.runner.DiaDBRunner;
import com.dianemodb.runner.ExampleRunner;
import com.dianemodb.runner.KafkaClientRunner;
import com.dianemodb.tpcc.schema.TpccBaseTable;

public class TpccDataPopulationGenerator {
	
	private static final String OLD_RECORD_ID_COLUMN_NAME = "old_record_id";
	private static final String NEW_RECORD_ID_COLUMN_NAME = "record_id";
	private static final String ID_SEQ_NAME = "id_sequence";
	private static final String WAREHOUSE_ID_COLUMN_NAME = "wh_id";
	
	public static void main(String[] args) throws Exception {
		Topology topology = DiaDBRunner.readTopologyFromFile(ExampleRunner.SMALL_SINGLE_LEVEL_TOPOLOGY);
		SQLServerApplication application = TpccRunner.createApplication(topology);

		populate(topology, application, 11);
	}
	
	public static void populate(
			Topology topology, 
			SQLServerApplication application,
			int multiplier
	) {
		List<ServerComputerId> leafComputers = topology.getLeafNodes();
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
		
		for( int i = 0; i < multiplier; i++ ) {
			for(ServerComputerId computerId : leafComputers) {
				byte index = computerId.getIndexValue();
				final int ii = i;
				
				executor.execute(
					() ->  {
						System.out.println("Populating " + computerId.clearTextFormat() + " loop " + ii);
						
						// this is the default directory, configured byt the environment switch
						StorageConnection connection = 
								new BenchmarkingH2ConnectionWrapper(
										KafkaClientRunner.ABSOLUTE_ROOT_DIRECTORY + "server_1", 
										computerId.clearTextFormat()
								);
						
						populate(
								application, 
								connection, 
								computerId, 
								(short) index, 
								(short) ((index + leafComputers.size()) * (ii + 1))
						);
								
						connection.commit();
						connection.close();
						
						System.out.println("Done for " + computerId.clearTextFormat() + " loop " + ii);
					}
				);				
			}
		}
	}

	public static void populate(
			SQLServerApplication application,
			StorageConnection connection,
			ServerComputerId computerId,
			short wh_id,
			short new_wh_id
	) {

		ServerComputerId id = new ServerComputerId(ServerComputerId.ROOT, (byte) 0);
		
		String serverId = id.clearTextFormat();
		
		// init this to be the current value
		String initSequence = 
				"CREATE SEQUENCE " + ID_SEQ_NAME + " start with ("
					+ "SELECT f.value FROM ID_FACTORY f WHERE f.name='user_rec_id' LIMIT 1"
				+ ")";
		
		SQLHelper.executeStatement(connection, initSequence);
		
		Collection<ServerTable<?>> tables = 
				application.tables()
					.values()
					.stream()
					.filter(t -> t instanceof UserRecordTable)
					.collect(Collectors.toList());
		
		for(ServerTable<?> table : tables) {
			table( (TpccBaseTable<?>) table, connection, serverId, wh_id, new_wh_id);
		}
		
		String updateIdFactoryStatement = 
				"UPDATE ID_FACTORY SET value = " + ID_SEQ_NAME + ".nextval "
						+ "WHERE name='" + TestComputer.USER_RECORD_ID_FACTORY_ID + "'";
		SQLHelper.executeStatement(connection, updateIdFactoryStatement);
		
		String dropIdSequenceStatement = "DROP SEQUENCE " + ID_SEQ_NAME + ";";
		SQLHelper.executeStatement(connection, dropIdSequenceStatement);
	}

	private static <R extends UserRecord> void table(
			TpccBaseTable<R> userTable, 
			StorageConnection connection,
			String serverId, 
			short wh_id,
			short new_wh_id
	) {	
		RecordColumn<R, Short> warehouseIdColumn = userTable.getWarehouseIdColumn();
		RecordColumn<R, RecordId> recordIdColumn = userTable.getRecordIdColumn();

		List<RecordColumn<R, ?>> userRecordColumns = userTable.getColumns();
		userRecordColumns.remove(recordIdColumn);
		userRecordColumns.remove(warehouseIdColumn);
		
		String commaSeparatedColumnNames = SQLHelper.getCommaSeparatedColumnNames(userRecordColumns);
		String userRecordTempTableName = userTable.getName() + "_tmp ";
		
		String recordIdDataType = recordIdColumn.getSQLDataType();
		
		List<String> nameAndValues = SQLHelper.convertColumnsToNameAndValue(userRecordColumns);
		nameAndValues.add(NEW_RECORD_ID_COLUMN_NAME + " " + recordIdDataType);
		nameAndValues.add(OLD_RECORD_ID_COLUMN_NAME + " " + recordIdDataType);
		nameAndValues.add(WAREHOUSE_ID_COLUMN_NAME + " " + warehouseIdColumn.getSQLDataType());
		
		String createTableStatement = 
				SQLHelper.createTableStatementFromStrings(
						userRecordTempTableName, 
						"CREATE GLOBAL TEMPORARY TABLE ", 
						nameAndValues
				);
		SQLHelper.executeStatement(connection, createTableStatement);
		
		
		String tmpInsertRecordsStatement =
			"INSERT INTO " + userRecordTempTableName + "(" 
					+ commaSeparatedColumnNames + ", " 
					+ NEW_RECORD_ID_COLUMN_NAME + ", " 
					+ OLD_RECORD_ID_COLUMN_NAME  + ", "
					+ WAREHOUSE_ID_COLUMN_NAME
				+ ")" 
				+ "SELECT " 
					+ commaSeparatedColumnNames + ", "
					+ "'" + serverId + ":' || " + ID_SEQ_NAME + ".nextval," 
					+ recordIdColumn.getName() + ","
					+ new_wh_id
				+ " FROM " + userTable.getName() 
				+ " WHERE " + userTable.getWarehouseIdColumn().getName() + "=" + wh_id + ";";
		
		SQLHelper.executeStatement(connection, tmpInsertRecordsStatement);
		
		String insertRecordsStatement = 
			"INSERT INTO " + userTable.getName() + "(" 
					+ commaSeparatedColumnNames + ", " 
					+ recordIdColumn.getName() + ","
					+ warehouseIdColumn.getName()
				+ ")"
				+ " SELECT " 
					+ commaSeparatedColumnNames + ", " 
					+ NEW_RECORD_ID_COLUMN_NAME + ", "
					+ WAREHOUSE_ID_COLUMN_NAME
				+ " FROM " + userTable.getName()+ "_tmp;";
		
		SQLHelper.executeStatement(connection, insertRecordsStatement);
		
		Set<IndexTable> indices = 
				userTable.allIndices()
					.values()
					.stream()
					.map(DistributedIndex::getTable)
					.collect(Collectors.toSet());
		
		for(IndexTable i : indices) {	
			List<RecordColumn<IndexRecord, ?>> indexColumnsWithoutUserRecord = i.getColumns();
			
			// record-id needs to be replaced with a new value
			indexColumnsWithoutUserRecord.remove(i.getRecordIdColumn());
			
			// user-record id needs to be the same as one just inserted 
			indexColumnsWithoutUserRecord.remove(i.getUserRecordColumn());
			
			String indexTmpTableName = i.getName() + "_tmp";
			
			// foo VARCHAR(128), bar INT
			List<String> indexColumnStringsWithValues = 
					SQLHelper.convertColumnsToNameAndValue(indexColumnsWithoutUserRecord);
			
			indexColumnStringsWithValues.add(
					NEW_RECORD_ID_COLUMN_NAME + " " + i.getRecordIdColumn().getSQLDataType());
			
			indexColumnStringsWithValues.add(
					i.getUserRecordColumn().getName() + " " + i.getUserRecordColumn().getSQLDataType());
			
			String createTempIndexTableStatement = 
					SQLHelper.createTableStatementFromStrings(
							indexTmpTableName, 
							"CREATE GLOBAL TEMPORARY TABLE ", 
							indexColumnStringsWithValues
					);
			
			SQLHelper.executeStatement(connection, createTempIndexTableStatement);
			
			String tmpIndexInsertStatement = 
				"INSERT INTO " + indexTmpTableName
					+ "(" 
						+ SQLHelper.getCommaSeparatedColumnNames(indexColumnsWithoutUserRecord) + ", "
						
						// the new record-id of the index-record
						+ NEW_RECORD_ID_COLUMN_NAME + ", "
						
						// the new user-record
						+ i.getUserRecordColumn().getName()
					+ ") "
					+ "SELECT " 
						+ SQLHelper.getCommaSeparatedColumnNamesWithPrefix("i", indexColumnsWithoutUserRecord) + ", "
						+ "('" + serverId + ":' || " + ID_SEQ_NAME  + ".nextval),"
						+ "u." + NEW_RECORD_ID_COLUMN_NAME
						
						+ " FROM " + i.getName() + " i " 
							+ " JOIN " + userRecordTempTableName + " u " 
								+ " ON u." + OLD_RECORD_ID_COLUMN_NAME + "=i." + i.getUserRecordColumn().getName() + ";";
			
			SQLHelper.executeStatement(connection, tmpIndexInsertStatement);
			
			String insertIndexStatement = 
				"INSERT INTO " + i.getName() + "(" 
					+ SQLHelper.getCommaSeparatedColumnNames(indexColumnsWithoutUserRecord) + ","
					+ i.getRecordIdColumn().getName() + ","
					+ i.getUserRecordColumn().getName()
				+ ") "
					+ "SELECT "
						+ SQLHelper.getCommaSeparatedColumnNamesWithPrefix("i", indexColumnsWithoutUserRecord) + ","
						+ "i." + NEW_RECORD_ID_COLUMN_NAME + "," 
						+ "i." + i.getUserRecordColumn().getName()
					+ " FROM " + indexTmpTableName + " i;";
			
			SQLHelper.executeStatement(connection, insertIndexStatement);
			
			String dropTempIndexTableStatement = "DROP TABLE " + indexTmpTableName + ";";
			SQLHelper.executeStatement(connection, dropTempIndexTableStatement);
		}
		
		String dropTempUserTableStatement = "DROP TABLE " + userRecordTempTableName + ";";
		
		SQLHelper.executeStatement(connection, dropTempUserTableStatement);
	}
	
}
