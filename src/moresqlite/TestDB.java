package moresqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

public class TestDB {

	@Test
	public void testinit() {

		StopWatch sw = new StopWatch();
		sw.start();
		sw.split();
		System.out.println("begin : " + sw.getSplitTime());

		int version = 0;
		Connection db = MyDB.getDB(version);
		int[] columnList = new int[] { 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008 };
		int[] indexList = new int[] { 1004 };

		List<Object[]> allrow = new ArrayList<Object[]>();
		for (int i = 0; i < 500000; i++) {

			Object[] objarr = new Object[columnList.length];

			for (int j = 0; j < columnList.length; j++) {
				objarr[j] = i + j;
			}

			allrow.add(objarr);
		}

		sw.split();
		System.out.println("int all row begin : " + sw.getSplitTime());

		try {
			int tableName = 1010;
			MyDB.createTable(db, version, tableName, columnList, indexList);
			MyDB.insertAllRow(db, version, tableName, columnList, allrow, 120);

			sw.split();
			System.out.println("int all row end : " + sw.getSplitTime());

			List<Map<String, String>> listMapFromTable = MyDB.getListMapFromTable(db, tableName);

			sw.split();
			System.out.println("get all row listmap end, size=" + listMapFromTable.size() + " " + sw.getSplitTime());

			List<Object[]> listFromTable = MyDB.getListFromTable(db, tableName);

			sw.split();
			System.out.println("get all row list end, size=" + listFromTable.size() + " " + sw.getSplitTime());

			MyDB.backupDB(db, version);

			sw.split();
			System.out.println("backup db end : " + sw.getSplitTime());

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
