package project.cs439.topology;

import backtype.storm.utils.DRPCClient;
import com.google.common.collect.Lists;
import project.cs439.function.CorrectionFunction;
import project.cs439.query.StatisticsQuery;
import project.cs439.query.StatisticsReducer;
import project.cs439.spout.SequenceReadStreamingSpout;
import project.cs439.state.StatisticsState;
import project.cs439.state.StatisticsUpdater;
import storm.trident.Stream;

import java.io.*;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import storm.trident.TridentState;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import backtype.storm.generated.StormTopology;
import storm.trident.spout.RichSpoutBatchExecutor;

public class ErrorCorrectorTopology {
    public static StormTopology buildTopology (LocalDRPC drpc, String drpcFunction, final ITridentSpout reader,
                                               int readLength)
    {
        final TridentTopology topology = new TridentTopology();
        final Stream sequenceStream = topology.newStream("sequence-reader", reader).parallelismHint(1);

        // In this state we save the histogram
        TridentState qMerStatistics = sequenceStream
                .partitionBy(new Fields("read"))
                .partitionPersist(new StatisticsState.StatisticsStateFactory(1000000, 15, readLength),
                                  new Fields("rownum", "read", "quality"), new StatisticsUpdater())
                .parallelismHint(16)
	;

        // Query the distributed histograms and aggregate.
        topology
                .newDRPCStream(drpcFunction, drpc).parallelismHint(1)
                .broadcast()  // for distributed query
                .stateQuery(qMerStatistics,
                            new Fields("args"),
                            new StatisticsQuery(), // Distributed Query for persistent state
                            new Fields("partialHistogram", "conditionalCounts", "positionalCounts"))
                .project(new Fields("partialHistogram", "conditionalCounts", "positionalCounts"))
                .aggregate(new Fields("partialHistogram", "conditionalCounts", "positionalCounts"),
                           new StatisticsReducer(), // Reduce statistics
                           new Fields("statistics"))
                .parallelismHint(1)
                .project(new Fields("statistics"))
                .broadcast() // Broadcast statistics to all partitions
                .each(new Fields("statistics"), new CorrectionFunction(),
                      new Fields("result"))
                .parallelismHint(16)
        ;

        return topology.build();
    }

    private static int getReadLength (String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        reader.readLine();
        int length = reader.readLine().length();
        reader.close();
        return length;
    }

    private static void cleanup (final DRPCClient client, final BufferedWriter writer) throws IOException {
        writer.close();
        client.close();
    }

    private static void saveResults (final BufferedWriter writer, final String result) throws IOException {
        writer.append(result);
        writer.newLine();
        writer.flush();
    }

    public static Config getStormConfig () {
        Config conf = new Config();
//        conf.setDebug(true);
        conf.setNumAckers(8);
        conf.setNumWorkers(8);
        conf.setMaxSpoutPending(100);
        conf.put("topology.spout.max.batch.size", 10000);
        conf.put("topology.trident.batch.emit.interval.millis", 100);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd1"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
	    conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 120);
//      conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5000);
//      conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 180000);
//      onf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 150000);
//      conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 900);
        conf.put(Config.DRPC_REQUEST_TIMEOUT_SECS, 1800);

        return conf;
    }

    public static void main (String[] args) throws Exception {

        Config config = getStormConfig();
        String[] fieldNames = {"rownum", "read", "quality"};

        SequenceReadStreamingSpout sequenceReader = new SequenceReadStreamingSpout(args[0], fieldNames);
        final ITridentSpout sequenceReadBatchExecutor = new RichSpoutBatchExecutor(sequenceReader);

        StormTopology topology = buildTopology(null, "EC", sequenceReadBatchExecutor, getReadLength(args[0]));
        StormSubmitter.submitTopology("EC", config, topology);
    }
}

