package project.cs439.state;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/3/13
 * Time: 8:40 AM
 */
public class HistogramUpdater implements StateUpdater<StatisticsState.Histogram> {
    private int localPartition;
    private int noOfPartitions;

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex();
        noOfPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup () {}

    private static void updateTrustedQmersAndStatistics (final StatisticsState.Histogram histogram,
                                                         final String kmer,
                                                         final double correctnessProbability)
    {
        if (histogram.getBloomFilter().mightContain(kmer)) {

            if (histogram.getTrustedQmers().containsKey(kmer)) {
                histogram.getTrustedQmers().put(kmer, histogram.getTrustedQmers().get(kmer) + 1D);
            } else {
                // This was already there in the bloom filter - count twice
                histogram.getTrustedQmers().put(kmer, 2D);
            }
        } else {
            histogram.getBloomFilter().put(kmer);
            // TODO: Is this assumption valid? What if errors have high correctness probability?
            // TODO: Now this is becoming philosophical. Go get a coffee!
            // TODO: If this is an SNP, the average quality must be high. So retain it.
            if (correctnessProbability > 0.999) {histogram.getTrustedQmers().put(kmer, 1.1D);}
        }
    }

    @Override
    public void updateState (final StatisticsState.Histogram histogram,
                             final List<TridentTuple> tridentTuples,
                             final TridentCollector tridentCollector)
    {
        for(TridentTuple tuple : tridentTuples){
            String kmer = tuple.getStringByField("kmer");
            String qmer = tuple.getStringByField("qmer");
            double averageQuality = StatisticsUpdater.getAverageQuality(qmer);
            double correctnessProbability = 1.0 - Math.pow(10.0, -0.1 * averageQuality);
            updateTrustedQmersAndStatistics(histogram, kmer, correctnessProbability);
        }
        System.out.println(MessageFormat.format("Debug: updated Histogram at partition " +
                                                        "[{0}] of [{1}]", localPartition, noOfPartitions));
    }
}
