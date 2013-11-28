package project.cs439.state;

import backtype.storm.task.IMetricsContext;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.io.Serializable;
import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.hash.BloomFilter.create;


public class StatisticsState implements State, Serializable {
    public StatisticsState () throws SQLException {
        k = 15;
        bloomFilter = create(Funnels.stringFunnel(), 100000, 0.001);
        trustedQmers = new Hashtable<String, Double>(100);

        positionalQualityCounts = new double[100][4]; //P,O
        positionalConditionalQualityCounts = new double[100][4][4]; // P,A,O

        jdbcConnection = getNewDatabaseConnection();
    }

    public StatisticsState (int expectedNumberOfElements, int k, int readLength) throws SQLException {
        this.k = k;
        bloomFilter = create(Funnels.stringFunnel(), expectedNumberOfElements, 0.001);
        trustedQmers = new Hashtable<String, Double>(100);

        positionalQualityCounts = new double[readLength][4]; //P,O
        positionalConditionalQualityCounts = new double[readLength][4][4]; // P,A,O

        jdbcConnection = getNewDatabaseConnection();
    }

    public static Connection getNewDatabaseConnection () throws SQLException {
        lookupDriver();
        Connection c = connectAndUseDatabase();
        setConnectionProperties(c);
        return c;
    }

    private static void setConnectionProperties (Connection connection) throws SQLException {
        connection.setAutoCommit(true);
        Statement stmt = connection.createStatement();
        if (stmt.execute(
                MessageFormat.format(
                        "create table if not exists {0}(rownum int, seqread varchar(200), phred varchar(200)) engine=innodb",
                        TABLE_NAME))) {
            stmt.execute(
                    MessageFormat.format(" ALTER TABLE {0} add index(seqread), add unique index(rownum)", TABLE_NAME));
        }
        stmt.close();
    }

    private static Connection connectAndUseDatabase () throws SQLException {
        Connection jdbcConnection;
        try {
            jdbcConnection = DriverManager.getConnection(DB_URL, "hive", "hive");
        } catch ( SQLException e ) {
            jdbcConnection = DriverManager.getConnection(GENERAL_URL, "hive", "hive");
            Statement stmt = jdbcConnection.createStatement();
            stmt.execute("create DATABASE IF NOT EXISTS datasets");
            stmt.execute("use datasets");
            stmt.close();
        }
        return jdbcConnection;
    }

    private static void lookupDriver () {
        try {
            System.out.println("Debug: Connecting to DB");
            Class.forName(JDBC_DRIVER);
        } catch ( ClassNotFoundException e ) {
            System.out.println("Error: Where is your MySQL JDBC Driver?");
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public static int getMaxReadId (final Connection jdbcConnection, final String tableName) throws SQLException {
	System.out.println("Debug: getMaxRead -- " + tableName);
        Statement stmt = jdbcConnection.createStatement();
        String sql = MessageFormat.format("SELECT max(rownum) as count FROM {0}", tableName);
        ResultSet rs = stmt.executeQuery(sql);
        int maxRow = 0;
        if (rs.next()) {
            maxRow = rs.getInt("count");
        }
        stmt.close();
        rs.close();
        return maxRow;
    }

    public static Map<String, Object> getAll (final Connection jdbcConnection, String tableName, int readId) throws
    SQLException
    {
	System.out.println("Debug: getAll -- " + tableName);
        Statement stmt = jdbcConnection.createStatement();
        String sql = MessageFormat.format("SELECT * FROM {0} where rownum = {1}", tableName, readId);
        ResultSet rs = stmt.executeQuery(sql);
        Map<String, Object> ret = new HashMap<String, Object>();
        if (rs.next()) {
            ret.put("rownum", rs.getInt("rownum"));
            ret.put("seqread", rs.getString("seqread"));
            ret.put("phred", rs.getString("phred"));
        }
        stmt.close();
        rs.close();
        return ret;
    }

    public static ResultSet getAll (final Connection jdbcConnection, String tableName, int start, int end) throws
    SQLException
    {
	
	System.out.println("Debug: getAll -- " + tableName);
        Statement stmt = jdbcConnection.createStatement();
        stmt.setFetchSize(100);
        stmt.setQueryTimeout(0);
        String sql = MessageFormat.format("SELECT * FROM {0} where rownum >= {1} and rownum <= {2}", tableName, start,
                                          end);
        return stmt.executeQuery(sql);
    }

    public static void updateCorrections (final Connection jdbcConnection, String tableName, String correction, int rownum) throws
    SQLException
    {
        Statement stmt = jdbcConnection.createStatement();
        stmt.setFetchSize(100);
        stmt.setQueryTimeout(0);
        String sql = MessageFormat.format("UPDATE {0} SET CORRECTED = {1} WHERE ROWNUM = {2}", tableName, correction, rownum);
        stmt.execute(sql);
        stmt.close();
    }

    public static void insert (final Connection jdbcConnection, Map<String, Object> row, String tableName) throws
    SQLException
    {
	System.out.println("Debug: insert -- " + row);
        Statement stmt = jdbcConnection.createStatement();
        StringBuilder sql = new StringBuilder();
        sql.append(MessageFormat.format("insert into {0} (rownum,seqread,phred,corrected) values (", tableName));
        for (Iterator<String> iterator = row.keySet().iterator(); iterator.hasNext(); ) {
            final String key = iterator.next();
            sql.append("'").append(row.get(key)).append("'");
            if (iterator.hasNext())
                sql.append(", ");
        }
        sql.append(")");
        stmt.execute(sql.toString());
        stmt.close();
    }

    public static int getNucleotideIndex (char ch) {
        switch (ch) {
            case 'A':
                return 0;
            case 'C':
                return 1;
            case 'G':
                return 2;
            case 'T':
                return 3;
            default:
                return -1;
        }
    }

    public static char getNucleotide (int i) {
        switch (i) {
            case 0:
                return 'A';
            case 1:
                return 'C';
            case 2:
                return 'G';
            case 3:
                return 'T';
            default:
                return 'N';
        }
    }

    public BloomFilter<CharSequence> getBloomFilter () {
        return bloomFilter;
    }

    public Hashtable<String, Double> getTrustedQmers () {
        return trustedQmers;
    }

    @Override
    public void commit (final Long aLong) {
        try {
            jdbcConnection.commit();
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }

    @Override
    public void beginCommit (final Long aLong) {}

    public Connection getJdbcConnection () {
        return jdbcConnection;
    }

    public static class StatisticsStateFactory implements StateFactory {

        private final int k, expectedNoOfElements, readLength;

        public StatisticsStateFactory (int expectedNoOfElements, int k, int readLength) {
            this.expectedNoOfElements = expectedNoOfElements;
            this.k = k;
            this.readLength = readLength;
        }

        public State makeState (Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            // our logic is fully idempotent => no Opaque Map or Transactional Map required here
            try {
                return new StatisticsState(expectedNoOfElements, k, readLength);
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static StateFactory FACTORY = new StateFactory() {
        public State makeState (Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            // our logic is fully idempotent => no Opaque Map or Transactional Map required here
            try {
                return new StatisticsState();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
            return null;
        }
    };

    public static int          k;
    public        double[][]   positionalQualityCounts;
    public        double[][][] positionalConditionalQualityCounts;

    private Connection                                       jdbcConnection;
    private Hashtable<String, Double>                        trustedQmers;
    private com.google.common.hash.BloomFilter<CharSequence> bloomFilter;

    public static final  String TABLE_NAME  = "ecoli";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String GENERAL_URL = "jdbc:mysql://qp-hd10:3306";
    private static final String DB_URL      = "jdbc:mysql://qp-hd10/datasets:3306";
}
