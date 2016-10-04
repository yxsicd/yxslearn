package moresqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

public class MyDB {

	private static final int _TestCOUNT = 5000000;
	private static final String SQL_PRE = "_";
	private static final String SQL_SPLIT = ".";

	public static String getVersionName(int version) {
		return SQL_PRE + version;
	}

	public static String getTableName(int tableName) {
		return SQL_PRE + tableName;
	}

	public static String getVersionTableName(int version, int tableName) {
		return SQL_PRE + version + SQL_SPLIT + SQL_PRE + tableName;
	}

	public static String getVersionColumnName(int version, int tableName, int columnName) {
		return SQL_PRE + version + SQL_SPLIT + SQL_PRE + tableName + SQL_SPLIT + SQL_PRE + columnName;
	}

	public static String getColumnName(int columnName) {
		return SQL_PRE + columnName;
	}

	public static String[] getVersionColumnNameList(int version, int tableName, int[] columnName) {
		String[] retList = new String[columnName.length];
		for (int i = 0; i < columnName.length; i++) {
			retList[i] = getVersionColumnName(version, tableName, columnName[i]);
		}
		return retList;
	}

	public static String[] getColumnNameList(int[] columnName) {
		String[] retList = new String[columnName.length];
		for (int i = 0; i < columnName.length; i++) {
			retList[i] = getColumnName(columnName[i]);
		}
		return retList;
	}

	public static String[] getColumnNamePatternList(int[] columnName) {
		String[] retList = new String[columnName.length];
		for (int i = 0; i < columnName.length; i++) {
			retList[i] = "?";
		}
		return retList;
	}

	public static Connection attachDB(Connection db, int[] versionList, int version) {

		try {
			for (int i = 0; i < versionList.length; i++) {
				if (versionList[i] == version) {
					continue;
				}
				String versionName = getVersionName(versionList[i]);
				String format = MessageFormat.format("attach database \"file:{0}?mode=memory&cache=shared\" as {0} ;",
						versionName);
				db.createStatement().executeUpdate(format);
			}
			System.out.println("create db ok");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return db;
	}

	public static Connection getDB(int dbName) {
		Connection connection = null;
		try {
			Class.forName("org.sqlite.JDBC");
			String url = MessageFormat.format("jdbc:sqlite:file:{0}?mode=memory&cache=shared", SQL_PRE + dbName);
			connection = DriverManager.getConnection(url);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}

	@Test
	public void mainTest() throws SQLException, InterruptedException, ExecutionException {
		int[] versionList = new int[] { 0, 1, 2, 3, 4 };

		final Connection db8 = getDB(8);
		final Connection db = getDB(0);
		final Connection db2 = getDB(1);
		final Connection db3 = getDB(2);
		final Connection db4 = getDB(3);
		final Connection db5 = getDB(4);

		attachDB(db8, versionList, 8);

		final int[] columnList_1010 = new int[] { 1002, 1004 };
		int[] columnList_2080 = new int[] { 1002, 1004, 1005 };

		createTable(db, 0, 1010, columnList_1010, new int[] { 1004 });
		createTable(db, 0, 2080, columnList_2080, new int[] { 1004 });

		createTable(db2, 1, 1010, columnList_1010, new int[] { 1004 });
		createTable(db2, 1, 2080, columnList_2080, new int[] { 1004 });

		createTable(db3, 2, 1010, columnList_1010, new int[] { 1004 });
		createTable(db3, 2, 2080, columnList_2080, new int[] { 1004 });

		createTable(db4, 3, 1010, columnList_1010, new int[] { 1004 });
		createTable(db4, 3, 2080, columnList_2080, new int[] { 1004 });

		createTable(db5, 4, 1010, columnList_1010, new int[] { 1004 });
		createTable(db5, 4, 2080, columnList_2080, new int[] { 1004 });

		StopWatch sw = new StopWatch();
		sw.start();
		sw.split();
		System.out.println("begin: " + sw.getSplitTime());

		db.setAutoCommit(false);
		db2.setAutoCommit(false);
		db3.setAutoCommit(false);
		db4.setAutoCommit(false);
		db5.setAutoCommit(false);

		Callable<Boolean> callable50 = getTask(db, columnList_1010, 1);
		Callable<Boolean> callable51 = getTask(db2, columnList_1010, 5);
		Callable<Boolean> callable52 = getTask(db3, columnList_1010, 10);
		Callable<Boolean> callable53 = getTask(db4, columnList_1010, 15);
		Callable<Boolean> callable54 = getTask(db5, columnList_1010, 300);

		ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
		ArrayList<Future<Boolean>> retList = new ArrayList<Future<Boolean>>();

		retList.add(newCachedThreadPool.submit(callable50));
		retList.add(newCachedThreadPool.submit(callable51));
		retList.add(newCachedThreadPool.submit(callable52));
		retList.add(newCachedThreadPool.submit(callable53));
		retList.add(newCachedThreadPool.submit(callable54));

		for (Future<Boolean> ret : retList) {
			ret.get();
		}

		db.commit();
		db2.commit();
		db3.commit();
		db4.commit();
		db5.commit();

		ResultSet executeQuery = db8.createStatement().executeQuery(
				"select (select count(1) from _0._1010)||'-'||(select count(1) from _1._1010)||'-'||(select count(1) from _2._1010)||'-'||(select count(1) from _3._1010)||'-'||(select count(1) from _4._1010)||'-' as ret");
		while (executeQuery.next()) {
			String string = executeQuery.getString(1);
			System.out.println(string);
		}

		sw.split();
		System.out.println("insertAllRow: " + sw.getSplitTime());
		// backupDB(db8, versionList, 0);
		sw.split();
		System.out.println("backupDB: " + sw.getSplitTime());
		System.out.println(db);
	}

	private Callable<Boolean> getTask(final Connection db5, final int[] columnList_1010, final int batchCount) {
		Callable<Boolean> callable5 = new Callable<Boolean>() {

			public Boolean call() throws Exception {

				StopWatch sw2 = new StopWatch();
				sw2.start();
				sw2.split();
				System.out.println("insert begin: " + " BC " + batchCount + "  " + sw2.getSplitTime());

				String batchInsert = getInsertSql(db5, 4, 1010, columnList_1010, batchCount);
				String oneInsert = getInsertSql(db5, 4, 1010, columnList_1010, 1);

				PreparedStatement batchInsertStatement = db5.prepareStatement(batchInsert);
				PreparedStatement oneInsertStatement = db5.prepareStatement(oneInsert);

				int leftCount = _TestCOUNT % batchCount;
				int bC = (_TestCOUNT - leftCount) / batchCount;

				for (int i = 0; i < bC; i++) {
					for (int j = 0; j < batchCount; j++) {
						batchInsertStatement.setObject(j * 2 + 1, i * batchCount + j);
						batchInsertStatement.setObject(j * 2 + 2, i * batchCount + j);
					}
					batchInsertStatement.executeUpdate();
				}

				for (int i = 0; i < leftCount; i++) {
					oneInsertStatement.setObject(1, bC * batchCount + i);
					oneInsertStatement.setObject(2, bC * batchCount + i * 1.0);
					oneInsertStatement.executeUpdate();
				}

				sw2.split();
				System.out.println("insert end: " + " BC " + batchCount + "  " + sw2.getSplitTime());

				return true;
			}
		};
		return callable5;
	}

	public static void createTable(Connection db, int[] versionList, int tableName, int[] columnList, int[] indexList)
			throws SQLException {
		for (int version : versionList) {
			createTable(db, version, tableName, columnList, indexList);
		}
	}

	public static PreparedStatement getInsertStatement(Connection db, int version, int tableName, int[] columnList,
			int batchCount) throws SQLException {
		PreparedStatement prepareStatement = db
				.prepareStatement(getInsertSql(db, version, tableName, columnList, batchCount));
		return prepareStatement;
	}

	public static String getInsertSql(Connection db, int version, int tableName, int[] columnList, int batchCount)
			throws SQLException {
		String columnListString = String.join(",", getColumnNameList(columnList));
		String columnpatternListString = String.join(",", getColumnNamePatternList(columnList));
		String oneBatch = "(" + columnpatternListString + ")";
		ArrayList<String> retList = new ArrayList<String>();
		for (int b = 0; b < batchCount; b++) {
			retList.add(oneBatch);
		}

		String format = MessageFormat.format("insert into {0} ({1}) values {2} ;", getTableName(tableName),
				columnListString, String.join(",", retList));
		return format;
	}

	public static void insertOneRow(PreparedStatement insertStatement, Object[] row) throws SQLException {
		for (int i = 0; i < row.length; i++) {
			insertStatement.setObject(i + 1, row[0]);
		}
		insertStatement.executeUpdate();
	}

	public static void insertAllRow(Connection db, int version, int tableName, int[] columnList, List<Object[]> allrow,
			int batchCount) throws SQLException {
		PreparedStatement batchStatement = getInsertStatement(db, version, tableName, columnList, batchCount);
		PreparedStatement oneStatement = getInsertStatement(db, version, tableName, columnList, 1);
		db.setAutoCommit(false);

		if (batchCount < 1) {
			batchCount = 1;
		}

		int rowLength = allrow.size();
		int leftCount = rowLength % batchCount;
		int batchStep = (rowLength - leftCount) / batchCount;
		int columnLength = columnList.length;

		for (int i = 0; i < batchStep; i++) {
			for (int j = 0; j < batchCount; j++) {
				for (int l = 0; l < columnLength; l++) {
					int paraIndex = j * columnLength + l + 1;
					int rowIndex = i * batchCount + j;
					batchStatement.setObject(paraIndex, allrow.get(rowIndex)[l]);
				}
			}
			batchStatement.executeUpdate();
		}

		for (int i = 0; i < leftCount; i++) {
			for (int l = 0; l < columnLength; l++) {
				int paraIndex = l + 1;
				int rowIndex = batchStep * batchCount + i;
				oneStatement.setObject(paraIndex, allrow.get(rowIndex)[l]);
			}
			oneStatement.executeUpdate();
		}
		db.commit();
	}

	public static void createTable(Connection db, int version, int tableName, int[] columnList, int[] indexList)
			throws SQLException {
		String versionTableName = getVersionTableName(version, tableName);
		String rtableName = getTableName(tableName);
		String[] columnNameList = getColumnNameList(columnList);
		String sql = MessageFormat.format("create table if not exists {0} ( {1} )", rtableName,
				String.join(",", columnNameList));
		db.createStatement().executeUpdate(sql);

		if (indexList != null && indexList.length > 0) {
			String[] indexNameList = getColumnNameList(indexList);
			String versionName = getVersionName(version);
			String retTableName = getTableName(tableName);
			String sql2 = MessageFormat.format("create index if not exists {2} on  {1} ( {2} )", versionName,
					retTableName, String.join(",", indexNameList));
			db.createStatement().executeUpdate(sql2);
		}
	}

	public static void backupDB(Connection db, int dbName, int pre) throws SQLException {

		String backupDBName = "main";
		if (dbName != -1) {
			backupDBName = SQL_PRE + dbName;
		}

		String sql = MessageFormat.format("backup {0} to target/{1}_{0}.db", backupDBName, pre);
		db.createStatement().executeUpdate(sql);
	}

	public static void backupDB(Connection db, int pre) throws SQLException {
		backupDB(db, -1, pre);
	}

	public static void backupDB(Connection db, int[] versionList, int pre) throws SQLException {
		for (int v : versionList) {
			backupDB(db, v, pre);
		}
	}

	public static List<Object[]> getListFromResultSet(ResultSet rset) throws SQLException {
		int columnCount = rset.getMetaData().getColumnCount();
		List<Object[]> retList = new ArrayList<Object[]>();
		while (rset.next()) {
			Object[] row = new Object[columnCount];
			for (int i = 0; i < columnCount; i++) {
				row[i] = rset.getObject(i+1);
			}
			retList.add(row);
		}
		return retList;
	}
	
	public static List<Map<String,String>> getListMapFromResultSet(ResultSet rset) throws SQLException {
		int columnCount = rset.getMetaData().getColumnCount();
		List<Map<String,String>> retList = new ArrayList<Map<String,String>>();
		
		String[] columnNameList = new String[columnCount];
		for(int i=0;i<columnCount;i++)
		{
			columnNameList[i] = rset.getMetaData().getColumnName(i+1);
		}
		
		while (rset.next()) {
			Map<String,String> row=new HashMap<String,String>();
			for (int i = 0; i < columnCount; i++) {
				row.put(columnNameList[i], rset.getString(i+1));
			}
			retList.add(row);
		}
		return retList;
	}
	

	public static ResultSet selectTable(Connection db, int tableName) throws SQLException {

		String sql = MessageFormat.format("select * from {0}", getTableName(tableName));
		return db.prepareStatement(sql).executeQuery();
	}

	public static ResultSet selectTable(Connection db, int version, int tableName) throws SQLException {
		String sql = MessageFormat.format("select * from {0}", getVersionTableName(version, tableName));
		return db.prepareStatement(sql).executeQuery();
	}
	
	public static List<Object[]> getListFromTable(Connection db, int tableName) throws SQLException {
			ResultSet selectTable = selectTable(db,tableName);
			return getListFromResultSet(selectTable);
	}
	
	public static List<Map<String,String>> getListMapFromTable(Connection db, int tableName) throws SQLException {
		ResultSet selectTable = selectTable(db,tableName);
		return getListMapFromResultSet(selectTable);
}

}
