package com.dianemodb.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.dianemodb.ServerComputerId;
import com.dianemodb.Topology;
import com.dianemodb.UserRecord;
import com.dianemodb.computertest.framework.TestComputer;
import com.dianemodb.integration.sqlwrapper.BenchmarkingH2ConnectionWrapper;
import com.dianemodb.metaschema.RecordColumn;
import com.dianemodb.metaschema.SQLHelper;
import com.dianemodb.metaschema.SQLServerApplication;
import com.dianemodb.metaschema.distributed.UserRecordIndex;
import com.dianemodb.metaschema.index.IndexRecord;
import com.dianemodb.metaschema.index.IndexTable;
import com.dianemodb.metaschema.schema.ServerTable;
import com.dianemodb.metaschema.schema.UserRecordTable;
import com.dianemodb.runner.DiaDBRunner;
import com.dianemodb.runner.ExampleRunner;
import com.dianemodb.runner.KafkaClientRunner;
import com.dianemodb.tpcc.schema.WarehouseBasedTable;

public class TpccDataPopulationGenerator {
	
	private static final int PARALLEL_LOAD_THREADS = 2;

	private static final String OLD_RECORD_ID_COLUMN_NAME = "old_record_id";
	private static final String NEW_RECORD_ID_COLUMN_NAME = "record_id";
	private static final String ID_SEQ_NAME = "id_sequence";
	private static final String WAREHOUSE_ID_COLUMN_NAME = "wh_id";
	
	private static final int NUMBER_OF_WAREHOUSES_TO_GENERATE_PER_COMPUTER = 74;
	private static final int NUMBER_OF_EXISTING_WAREHOUSES = 1;
	
	public static void main(String[] args) throws Exception {
		Topology topology = DiaDBRunner.readTopologyFromFile(ExampleRunner.SMALL_SINGLE_LEVEL_TOPOLOGY);
		SQLServerApplication application = TpccRunner.createApplication(topology);

		populate(topology, application, NUMBER_OF_WAREHOUSES_TO_GENERATE_PER_COMPUTER);
	}
	
	public static void populate(
			Topology topology, 
			SQLServerApplication application,
			int multiplier
	) {
		List<ServerComputerId> leafComputers = topology.getLeafNodes();
		
		CountDownLatch latch = new CountDownLatch(multiplier * leafComputers.size()); 
		
		ThreadPoolExecutor executor = 
				(ThreadPoolExecutor) Executors.newFixedThreadPool(
						PARALLEL_LOAD_THREADS, 
						r -> { 
							Thread t = new Thread(r);
							t.setDaemon(true);
							return t;
						}
					);

		AtomicBoolean goon = new AtomicBoolean(true);

		OUTER:
		for( int i = 0; i < multiplier; i++ ) {
			for(ServerComputerId computerId : leafComputers) {
				if(!goon.get()) {
					break OUTER;
				}
				
				final int ii = i;

				short oldWarehouseId = (short) computerId.getIndexValue();
				short newWarehouseId = (short) ((oldWarehouseId + (leafComputers.size() * (ii + NUMBER_OF_EXISTING_WAREHOUSES))));
				
				executor.execute( 
					() ->  {
						try {
							run(application, computerId, ii, oldWarehouseId, newWarehouseId);
							latch.countDown();
						} catch (SQLException e) {
							executor.shutdownNow();
							goon.set(false);
							throw new RuntimeException(e);
						}
					}
				);
			}
		}
		
		if(goon.get()) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static void run(
			SQLServerApplication application, 
			ServerComputerId computerId, 
			final int ii, 
			short oldWarehouseId,
			short newWarehouseId
	) throws SQLException {
		System.out.println("Populating " + computerId.clearTextFormat() + " wh " + newWarehouseId);
		
		List<String> statements = 
			populate(
				application, 
				computerId, 
				oldWarehouseId, 
				newWarehouseId
			);

		// this is the default directory, configured byt the environment switch
		Connection connection = 
				BenchmarkingH2ConnectionWrapper.createConnection(
						KafkaClientRunner.ABSOLUTE_ROOT_DIRECTORY + "server_1", 
						computerId.clearTextFormat()
				);
		
		executeStatement(
				connection, 
				statements.toArray(new String[statements.size()])
		);
				
		connection.commit();
		connection.close();
		

		// this should trigger compacting and at worst fails ASAP if H2 would crash
		connection = 
				BenchmarkingH2ConnectionWrapper.createConnection(
						KafkaClientRunner.ABSOLUTE_ROOT_DIRECTORY + "server_1", 
						computerId.clearTextFormat()
				);
		
		connection.close();

		System.out.println("Done for " + computerId.clearTextFormat() + " loop " + ii);
	}
	
	public static void executeStatement(Connection conn, String... statements) throws SQLException {
		for(String statement : statements) {
			PreparedStatement jdbcStatement = conn.prepareStatement(statement);
			jdbcStatement.execute();
			jdbcStatement.close();
		}
	}

	public static List<String> populate(
			SQLServerApplication application,
			ServerComputerId computerId,
			short wh_id,
			short new_wh_id
	) {
		List<String> statements = new LinkedList<String>();
		
		String serverId = computerId.clearTextFormat();
		
		// init this to be the current value
		String initSequence = 
				"CREATE SEQUENCE " + ID_SEQ_NAME + " start with ("
					+ "SELECT f.value FROM ID_FACTORY f "
					+ " WHERE f.name='" + TestComputer.USER_RECORD_ID_FACTORY_ID + "' "
					+ " LIMIT 1"
				+ ")";
		
		statements.add(initSequence);
		
		Collection<ServerTable<?>> tables = 
				application.tables()
					.values()
					.stream()
					.filter(t -> t instanceof UserRecordTable)
					.collect(Collectors.toList());
		
		for(ServerTable<?> table : tables) {
			// the records which aren't aligned with warehouses should be ignored
			if(!(table instanceof WarehouseBasedTable)) {
				continue;
			}
			
			List<String> tableStatements = table( (WarehouseBasedTable<?>) table, serverId, wh_id, new_wh_id);
			statements.addAll(tableStatements);
		}
		
		String updateIdFactoryStatement = 
				"UPDATE ID_FACTORY SET value = " + ID_SEQ_NAME + ".nextval "
						+ "WHERE name='" + TestComputer.USER_RECORD_ID_FACTORY_ID + "'";
		
		statements.add(updateIdFactoryStatement);
		
		String dropIdSequenceStatement = "DROP SEQUENCE " + ID_SEQ_NAME;
		statements.add(dropIdSequenceStatement);
		
		return statements;
	}

	private static <R extends UserRecord> List<String> table(
			WarehouseBasedTable<R> userTable, 
			String serverId, 
			short wh_id,
			short new_wh_id
	) {	
		List<String> statements = new LinkedList<String>();
		
		RecordColumn<R, Short> warehouseIdColumn = userTable.getWarehouseIdColumn();
		RecordColumn<R, Long> recordIdColumn = userTable.getRecordIdColumn();

		List<RecordColumn<R, ?>> userRecordColumns = userTable.getColumns();
		userRecordColumns.remove(recordIdColumn);
		userRecordColumns.remove(warehouseIdColumn);
		
		String userRecordTempTableName = userTable.getName() + "_tmp ";
		
		/*
		 * create headings for temp-table, like "foo VARCHAR(16), bar INT"
		 */
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
		
		statements.add(createTableStatement);
		
		String commaSeparatedColumnNames = SQLHelper.getCommaSeparatedColumnNames(userRecordColumns);

		String tmpInsertRecordsStatement =
			"INSERT INTO " + userRecordTempTableName + "(" 
					+ commaSeparatedColumnNames + ", " 
					+ NEW_RECORD_ID_COLUMN_NAME + ", " 
					+ OLD_RECORD_ID_COLUMN_NAME  + ", "
					+ WAREHOUSE_ID_COLUMN_NAME
				+ ")" 
				+ "SELECT " 
					+ commaSeparatedColumnNames + ", "
					+ ID_SEQ_NAME + ".nextval," 
					+ recordIdColumn.getName() + ","
					+ new_wh_id
				+ " FROM " + userTable.getName() 
				+ " WHERE " + warehouseIdColumn.getName() + "=" + wh_id;
		
		statements.add(tmpInsertRecordsStatement);
		
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
				+ " FROM " + userRecordTempTableName;
		
		statements.add(insertRecordsStatement);
		
		Set<UserRecordIndex<R>> indices =
				userTable.getMaintainingComputerDecidingIndex()
					.map( 
						i -> userTable.allIndices()
								.values()
								.stream()
								.collect(Collectors.toSet())
					)
					.orElse(Set.of());
		
		for(UserRecordIndex<R> i : indices) {	
			List<String> indexStrings = index(userTable, serverId, userRecordTempTableName, i, new_wh_id);
			statements.addAll(indexStrings);
		}
		
		String dropTempUserTableStatement = "DROP TABLE " + userRecordTempTableName;
		statements.add(dropTempUserTableStatement);

		return statements;
	}

	private static <R extends UserRecord> List<String> index(
				WarehouseBasedTable<R> userTable,
				String serverId, 
				String userRecordTempTableName,
				UserRecordIndex<R> index,
				short warehouseId
	) {
		IndexTable indexTable = index.getTable();
		
		RecordColumn<R, Short> warehouseColumn = userTable.getWarehouseIdColumn();
		
		List<String> statements = new LinkedList<String>();

		List<RecordColumn<IndexRecord, ?>> indexColumnsWithoutUserRecordAndWarehouse = indexTable.getColumns();

		// we have indices which have no warehouse-columns, like record-id & tx-id
		Optional<RecordColumn<IndexRecord, ?>> maybeIndexWarehouseColumn = 
				indexTable.getColumns()
					.stream()
					.filter( c -> c.getName().equals(warehouseColumn.getName()))
					.findAny();
		
		maybeIndexWarehouseColumn.ifPresent(c -> indexColumnsWithoutUserRecordAndWarehouse.remove(c));
		
		// record-id needs to be replaced with a new value
		indexColumnsWithoutUserRecordAndWarehouse.remove(indexTable.getRecordIdColumn());
		
		// user-record id needs to be the same as one just inserted 
		indexColumnsWithoutUserRecordAndWarehouse.remove(indexTable.getUserRecordColumn());
		
		/*
		 * we presume that there are other columns in the index beside the warehouse, as the 
		 * others are technical columns, so don't carry information for the end-user. 
		 */
		assert !indexColumnsWithoutUserRecordAndWarehouse.isEmpty() : index;
		
		String indexTmpTableName = indexTable.getName() + "_tmp";
		
		// foo VARCHAR(128), bar INT
		List<String> indexColumnStringsWithValues = 
				SQLHelper.convertColumnsToNameAndValue(indexColumnsWithoutUserRecordAndWarehouse);
		
		indexColumnStringsWithValues.add(
				NEW_RECORD_ID_COLUMN_NAME + " " + indexTable.getRecordIdColumn().getSQLDataType());
		
		indexColumnStringsWithValues.add(
				indexTable.getUserRecordColumn().getName() + " " + indexTable.getUserRecordColumn().getSQLDataType());
		
		String createTempIndexTableStatement = 
				SQLHelper.createTableStatementFromStrings(
						indexTmpTableName, 
						"CREATE GLOBAL TEMPORARY TABLE ", 
						indexColumnStringsWithValues
				);
		
		statements.add(createTempIndexTableStatement);
		
		String tmpIndexInsertStatement = 
			"INSERT INTO " + indexTmpTableName
				+ "(" 
					+ SQLHelper.getCommaSeparatedColumnNames(indexColumnsWithoutUserRecordAndWarehouse) + ", "
					
					// the new record-id of the index-record
					+ NEW_RECORD_ID_COLUMN_NAME + ", "
					
					// the new user-record
					+ indexTable.getUserRecordColumn().getName()
				+ ") "
				+ "SELECT " 
					+ SQLHelper.getCommaSeparatedColumnNamesWithPrefix("i", indexColumnsWithoutUserRecordAndWarehouse) + ", "
					+ ID_SEQ_NAME  + ".nextval,"
					
					// user-record id is text with server prefix
					+ "('" + serverId + ":' || u." + NEW_RECORD_ID_COLUMN_NAME + ")"
					+ " FROM " + userRecordTempTableName + " u " 
					+ " JOIN "  + indexTable.getName() + " i "
						+ " ON ('" + serverId + ":' || u." + OLD_RECORD_ID_COLUMN_NAME + ")=i." + indexTable.getUserRecordColumn().getName();
		
		statements.add(tmpIndexInsertStatement);
		
		String insertHeader = 							
				"INSERT INTO " + indexTable.getName() + "(" 
					+ SQLHelper.getCommaSeparatedColumnNames(indexColumnsWithoutUserRecordAndWarehouse) + ","
					+ indexTable.getRecordIdColumn().getName() + ","
					+ indexTable.getUserRecordColumn().getName();
		
		String selectHeader = 
				"SELECT "
					+ SQLHelper.getCommaSeparatedColumnNamesWithPrefix("i", indexColumnsWithoutUserRecordAndWarehouse) + ","
					+ "i." + NEW_RECORD_ID_COLUMN_NAME + "," 
					+ "i." + indexTable.getUserRecordColumn().getName();
		
		String insertIndexStatement =
				maybeIndexWarehouseColumn
					.map(
						indexWarehouseColumn -> 
							insertHeader + "," + indexWarehouseColumn.getName() + ") "
							+ selectHeader  + "," + warehouseId	+ " FROM " + indexTmpTableName + " i"
					)
					.orElseGet(
						() -> 
							insertHeader + ") " + selectHeader + " FROM " + indexTmpTableName + " i"
					);
		statements.add(insertIndexStatement);

		String dropTempIndexTableStatement = "DROP TABLE " + indexTmpTableName;
		statements.add(dropTempIndexTableStatement);

		return statements;
	}
}
