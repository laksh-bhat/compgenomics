package project.cs439.topology;

import backtype.storm.utils.DRPCClient;
import com.google.common.collect.Lists;
import project.cs439.function.CorrectionFunction;
import project.cs439.query.statisticsQuery;
import project.cs439.query.StatisticsReducer;
import project.cs439.spout.SequenceReadStreamingSpout;
import project.cs439.spout.SynchronousSequenceReadSpout;
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
        final Stream sequenceStream = topology.newStream("sequence-reader", reader);

        // In this state we save the histogram
        TridentState qMerStatistics = sequenceStream
                .partitionBy(new Fields("read"))
                .partitionPersist(new StatisticsState.StatisticsStateFactory(100000, 15, readLength),
                                  new Fields("rownum", "read", "quality"), new StatisticsUpdater())
                .parallelismHint(2);


        // Query the distributed histograms and aggregate.
        topology
                .newDRPCStream(drpcFunction, drpc)
                .broadcast()  // for distributed query
                .stateQuery(qMerStatistics,
                            new Fields("args"),
                            new statisticsQuery(), // Distributed Query for persistent state
                            new Fields("partialHistogram", "conditionalCounts", "positionalCounts"))
                .parallelismHint(2)
                .project(new Fields("partialHistogram", "conditionalCounts", "positionalCounts"))
                .aggregate(new Fields("partialHistogram", "conditionalCounts", "positionalCounts"),
                           new StatisticsReducer(), // Reduce statistics
                           new Fields("histogram", "conditionalCounts", "positionalCounts"))
                .project(new Fields("histogram", "conditionalCounts", "positionalCounts"))
                .broadcast() // Broadcast statistics to all partitions
                .each(new Fields("histogram", "conditionalCounts", "positionalCounts"), new CorrectionFunction(),
                      new Fields("result"))
                .parallelismHint(2)
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
        conf.setNumAckers(2);
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(2);
        conf.put("topology.spout.max.batch.size", 10);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd1"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        //conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 300);
        return conf;
    }

    public static void main (String[] args) throws Exception {

        Config config = getStormConfig();
        String[] fieldNames = {"rownum", "read", "quality"};

        SequenceReadStreamingSpout sequenceReader = new SequenceReadStreamingSpout(args[0], fieldNames);
        final ITridentSpout sequenceReadBatchExecutor = new RichSpoutBatchExecutor(sequenceReader);

        StormTopology topology = buildTopology(null, "EC", sequenceReadBatchExecutor, getReadLength(args[0]));
        StormSubmitter.submitTopology("EC", config, topology);

        // Let's wait until the entire file is processed
        while (!sequenceReader.isDone()) Thread.sleep(5000);

        DRPCClient client = new DRPCClient("localhost", 3772, 5000000);
        String result = client.execute("EC", "Query");
        BufferedWriter writer = new BufferedWriter(new FileWriter("results.dat"));
        saveResults(writer, result);
        cleanup(client, writer);
    }
}


