package project.cs439.topology;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import project.cs439.function.CorrectionFunction;
import project.cs439.query.*;
import project.cs439.spout.SequenceReadStreamingSpout;
import project.cs439.state.HistogramUpdater;
import project.cs439.state.StatisticsState;
import project.cs439.state.StatisticsUpdater;
import storm.trident.Stream;

import java.io.*;
import java.text.MessageFormat;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import storm.trident.TridentState;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;
import storm.trident.TridentTopology;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.ITridentSpout;
import backtype.storm.generated.StormTopology;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.tuple.TridentTuple;

public class ErrorCorrectorTopology {
    public static StormTopology buildTopology (LocalDRPC drpc, String drpcFunction, final ITridentSpout reader,
                                               int readLength)
    {
        int parallelismHint = 16;
        final TridentTopology topology = new TridentTopology();
        final Stream sequenceStream = topology.newStream("sequence-reader", reader).parallelismHint(1);
        final Stream kMerStream = sequenceStream.parallelismHint(1)
                .each(new Fields("rownum", "read", "quality"), new KmerSplitFilter(), new Fields("kmer", "qmer"))
		.project(new Fields("rownum", "kmer", "qmer"))
                .parallelismHint(parallelismHint);

        final TridentState histogram = kMerStream
                .parallelismHint(parallelismHint)
                .partitionBy(new Fields("kmer"))
                .partitionPersist(new StatisticsState.HistogramStateFactory(1000000/parallelismHint, 15),
                                  new Fields("rownum", "kmer", "qmer"),
                                  new HistogramUpdater())
                .parallelismHint(parallelismHint)
        ;

        // In this state we save the histogram
        TridentState qMerStatistics = sequenceStream
                .partitionBy(new Fields("read"))
                .partitionPersist(new StatisticsState.StatisticsStateFactory(1000000/parallelismHint, 15, readLength),
                                  new Fields("rownum", "read", "quality"), new StatisticsUpdater())
                .parallelismHint(parallelismHint)
	    ;

	Stream drpcStream = topology.newDRPCStream("EC", drpc).parallelismHint(1);

        // Query the distributed histograms and aggregate.
        Stream combinedHistogram = drpcStream
                .broadcast() // for distributed query
                .stateQuery(histogram,
                            new Fields("args"),
                            new HistogramQuery(),
                            new Fields("partialHistogram"))
                .project(new Fields("partialHistogram"))
                .aggregate(new Fields("partialHistogram"),
                           new HistogramCombiner(),
                           new Fields("histogram"))
        ;

        // Query the distributed counts and aggregate.
        Stream combinedCounts = drpcStream
                .broadcast()  // for distributed query
                .stateQuery(qMerStatistics,
                            new Fields("args"),
                            new StatisticsQuery(), // Distributed Query for persistent state
                            new Fields("conditionalCounts", "positionalCounts"))
                .project(new Fields("conditionalCounts", "positionalCounts"))
                .aggregate(new Fields("conditionalCounts", "positionalCounts"),
                           new StatisticsCombiner(), // Combine statistics
                           new Fields("statistics"))
        ;

        topology.multiReduce(combinedHistogram,
                             combinedCounts,
                             new MultiStatsReducer(),
                             new Fields("histogram", "conditionalCounts", "positionalCounts"))
                .each(new Fields("histogram", "conditionalCounts", "positionalCounts"),
                      new CorrectionFunction(),
                      new Fields("result"))
         	.parallelismHint(parallelismHint)
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

    public static Config getStormConfig () {
        Config conf = new Config();
//      conf.setDebug(true);
        conf.setNumAckers(8);
        conf.setNumWorkers(8);
        conf.setMaxSpoutPending(32);
        conf.put("topology.spout.max.batch.size", 25000);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6", "qp-hd7", "qp-hd8", "qp-hd9"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
//	conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 120);
//      conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5000);
//      conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 180000);
//      onf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 150000);
//      conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
//      conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
//      conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 900);
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

    private static class KmerSplitFilter implements Function {
        int localPartition, noOfPartitions;
        @Override
        public void execute (final TridentTuple readStreamElement, final TridentCollector tridentCollector) {

            int rowNum = readStreamElement.getIntegerByField("rownum");
            String read = readStreamElement.getStringByField("read");
            String qualities = readStreamElement.getStringByField("quality");

            for (int i = 0; i + StatisticsState.k < read.length(); i++) {
                String kmer = read.substring(i, i + StatisticsState.k);
                String qmer = qualities.substring(i, i + StatisticsState.k);
                //double averageQuality = StatisticsUpdater.getAverageQuality(qmer);

                // confidence < 50%, this is a bit aggressive, but that's Ok.
                // If we can't learn anything about these kmers, just discard them.
                //if (averageQuality < 3)
                //    continue;

                tridentCollector.emit(new Values(kmer, qmer));
            }
            //System.out.println(MessageFormat.format("Debug: Kmer Split at partition [{0}] of [{1}]", localPartition, noOfPartitions));
        }

        @Override
        public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex();
            noOfPartitions = tridentOperationContext.numPartitions();
        }

        @Override
        public void cleanup () {}
    }
}

