package project.cs439.state;

import backtype.storm.task.IMetricsContext;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.tuple.Pair;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.io.Serializable;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.hash.BloomFilter.create;


public class StatisticsState implements State, Serializable {
    public StatisticsState () throws SQLException {
        StatisticsState.k = 15;
        bloomFilter = create(Funnels.stringFunnel(), 100000, 0.001);
        trustedQmers = new ConcurrentHashMap<String, Double>(100);

        positionalQualityCounts = new double[100][5]; //P,O
        positionalConditionalQualityCounts = new double[100][5][5]; // P,A,O

        jdbcConnection = getNewDatabaseConnection();
        seenTuples = new CopyOnWriteArraySet<Integer>();
    }

    public StatisticsState (int expectedNumberOfElements, int k, int readLength) throws SQLException {
        StatisticsState.k = k;
        bloomFilter = create(Funnels.stringFunnel(), expectedNumberOfElements, 0.001);
        trustedQmers = new Hashtable<String, Double>(100);

        positionalQualityCounts = new double[readLength][5]; //P,O
        positionalConditionalQualityCounts = new double[readLength][5][5]; // P,A,O

        jdbcConnection = getNewDatabaseConnection();
        seenTuples = new CopyOnWriteArraySet<Integer>();
    }

    public static Connection getNewDatabaseConnection () throws SQLException {
        lookupDriver();
        Connection c = connectAndUseDatabase();
        setConnectionProperties(c);
        return c;
    }

    private static void setConnectionProperties (Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();
        if (stmt.execute(
                MessageFormat.format(
                        "create table if not exists {0}(rownum int, seqread varchar(200), phred varchar(200), corrected varchar(200)) engine=innodb",
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

    public static void saveBatchInDb (final Map<Integer, Pair<String, String>> dbPushMap,
                                      final StatisticsState statisticsState)
    {
        try {
            // Create a prepared statement
            String sql = MessageFormat.format("insert ignore into {0} set rownum = ?, seqread= ?, phred = ?, corrected = ?", StatisticsState.TABLE_NAME);
            PreparedStatement pstmt = statisticsState.getJdbcConnection().prepareStatement(sql);

            for (Integer rownum : dbPushMap.keySet()) {
                pstmt.setInt(1, rownum);
                pstmt.setString(2, dbPushMap.get(rownum).getLeft());
                pstmt.setString(3, dbPushMap.get(rownum).getRight());
                pstmt.setString(4, "");
                pstmt.addBatch();
            }
            // Execute the batch
            pstmt.executeBatch();
            statisticsState.getJdbcConnection().commit();
            pstmt.close();
        } catch ( SQLException ignore ) {}
    }


    public static int getMaxReadId (final Connection jdbcConnection, final String tableName) throws SQLException {
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
        Statement stmt = jdbcConnection.createStatement();
        String sql = MessageFormat.format("SELECT * FROM {0} where rownum = {1}", tableName, String.valueOf(readId));
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
        Statement stmt = jdbcConnection.createStatement();
        stmt.setFetchSize(100);
        stmt.setQueryTimeout(0);
        String sql = MessageFormat.format("SELECT * FROM {0} where rownum >= {1} and rownum <= {2} and corrected = '''' ", tableName, String.valueOf(start),
                                          String.valueOf(end));
        return stmt.executeQuery(sql);
    }



    public static void updateCorrections (final Connection jdbcConnection,
                                          String tableName,
                                          Map<Integer, String> corrections) throws
    SQLException
    {
        try {
            // Create a prepared statement
            String sql = MessageFormat.format("UPDATE {0} SET CORRECTED = ? WHERE ROWNUM = ?", tableName);
            PreparedStatement statement = jdbcConnection.prepareStatement(sql);

            for (Integer rowNum : corrections.keySet()) {
                statement.setString(1, corrections.get(rowNum));
                statement.setInt(2, rowNum);
                statement.addBatch();
            }
            // Execute the batch
            statement.executeBatch();
            statement.close();
        } catch ( SQLException ignore ) {}
    }

    public static void insert (final Connection jdbcConnection, Map<String, Object> row, String tableName) throws
    SQLException
    {
        Statement stmt = jdbcConnection.createStatement();
        StringBuilder sql = new StringBuilder();
        sql.append(MessageFormat.format("insert into {0} ( ", tableName));

        for (Iterator<String> iterator = row.keySet().iterator(); iterator.hasNext(); ) {
            final String column = iterator.next();
            sql.append(column);
            if (iterator.hasNext()) sql.append(", ");
        }
        sql.append(") values (");

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
                return 4;
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

    public Map<String, Double> getTrustedQmers () {
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

    public Set<Integer> getSeenTuples () {
        return seenTuples;
    }

    public static class StatisticsStateFactory implements StateFactory {

        private final int k, expectedNoOfElements, readLength;

        public StatisticsStateFactory (int expectedNoOfElements, int k, int readLength) {
            this.expectedNoOfElements = expectedNoOfElements;
            this.k = k;
            this.readLength = readLength;
        }

        public State makeState (Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
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
    private Set<Integer>                                     seenTuples;
    private Map<String, Double>                              trustedQmers;
    private com.google.common.hash.BloomFilter<CharSequence> bloomFilter;

    public static final  String TABLE_NAME  = "ecoli";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String GENERAL_URL = "jdbc:mysql://qp-hd10:3306";
    private static final String DB_URL      = "jdbc:mysql://qp-hd10:3306/datasets?useServerPrepStmts=false&rewriteBatchedStatements=true";
}
