import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TestInsert {


    public byte[] LongToBytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    public String getRandom() {
        byte[] bytes = new byte[32];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public String getSortRandom() {
        byte[] retbytes = new byte[32];
        byte[] bytes = new byte[24];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(bytes);
        System.arraycopy(LongToBytes(System.currentTimeMillis()), 0, retbytes, 0, 8);
        System.arraycopy(bytes, 0, retbytes, 8, 24);
        return Base64.getEncoder().encodeToString(retbytes);
    }


    @Test
    public void testInsert() throws SQLException {
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String password = "ascent";
        Connection conn = DriverManager.getConnection(url, user, password);

        String insert_unsort = "insert into unsortTable(id,id2, uuid, uuid2) values(?,?,?,?);";
        String insert_sort = "insert into sortTable(id, id2, uuid, uuid2) values(?,?,?,?);";
        PreparedStatement unsort_pre = conn.prepareStatement(insert_unsort);
        PreparedStatement sort_pre = conn.prepareStatement(insert_sort);

        conn.createStatement().execute("delete from unsortTable;");
        conn.createStatement().execute("delete from sortTable;");


        List<String> idlist = new ArrayList<>();
        List<String> id2list = new ArrayList<>();
        List<String> sortidlist = new ArrayList<>();
        List<String> unsortlist = new ArrayList<>();

        int testCount = 3000;
        for (int i = 0; i < testCount; i++) {
            idlist.add(getRandom());
            id2list.add(getRandom());
            sortidlist.add(getSortRandom());
            unsortlist.add(getRandom());
        }

        insert_test(sort_pre, idlist, id2list, sortidlist, testCount, "sortidlist=:");
        insert_test(unsort_pre, idlist, id2list, unsortlist, testCount, "unsortlist=:");

        conn.createStatement().execute("delete from unsortTable;");
        conn.createStatement().execute("delete from sortTable;");

        insert_test(unsort_pre, idlist, id2list, unsortlist, testCount, "unsortlist=:");
        insert_test(sort_pre, idlist, id2list, sortidlist, testCount, "sortidlist=:");

    }

    private void insert_test(PreparedStatement unsort_pre, List<String> idlist, List<String> id2list, List<String> unsortlist, int testCount, String s) throws SQLException {
        long beginTime2 = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            unsort_pre.setString(1, idlist.get(i));
            unsort_pre.setString(2, id2list.get(i));
            unsort_pre.setString(3, unsortlist.get(i));
            unsort_pre.setString(4, unsortlist.get(i));
            unsort_pre.execute();
            unsort_pre.clearParameters();
        }
        System.out.println(s + (System.currentTimeMillis() - beginTime2));
    }


//    sortidlist=:13333
//    unsortlist=:13663
//    unsortlist=:13156
//    sortidlist=:13542

//    sortidlist=:13079
//    unsortlist=:13502
//    unsortlist=:13530
//    sortidlist=:13844
}
