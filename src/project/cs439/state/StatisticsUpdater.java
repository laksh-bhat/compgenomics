package project.cs439.state;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: lbhat@damsl
 * Date: 11/21/13
 * Time: 7:42 PM
 */
public class StatisticsUpdater extends BaseStateUpdater<StatisticsState> {
    @Override
    public void updateState (final StatisticsState statisticsState, final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        Map<Integer, Pair<String, String>> dbPushMap = new ConcurrentHashMap<Integer, Pair<String, String>>();
        for (TridentTuple readStreamElement : tuples) {
            int rowNum = readStreamElement.getIntegerByField("rownum");
            if (statisticsState.getSeenTuples().contains(rowNum))
                continue;

            String read = readStreamElement.getStringByField("read");
            String qualities = readStreamElement.getStringByField("quality");

            dbPushMap.put(rowNum, new ImmutablePair<String, String>(read, qualities));
            learnAndFilterErrors(statisticsState, read, qualities);
            statisticsState.getSeenTuples().add(rowNum);
        }
        System.out.println(MessageFormat.format("Debug: StatisticsUpdater: Updated batch of {0} reads", dbPushMap.size()));

        StatisticsState.saveBatchInDb(dbPushMap, statisticsState);
        dbPushMap.clear();
    }


    private static void learnAndFilterErrors (final StatisticsState statisticsState,
                                              final String read,
                                              final String qualities)
    {
        int i;
        for (i = 0; i + StatisticsState.k < read.length(); i++) {
            String kmer = read.substring(i, i + StatisticsState.k);
            String qmer = qualities.substring(i, i + StatisticsState.k);
            double averageQuality = getAverageQuality(qmer);
            double correctnessProbability = 1.0 - Math.pow(10.0, -0.1 * averageQuality);

            // confidence < 50%, this is a bit aggressive, but that's Ok.
            // If we can't learn anything about these kmers, just discard them.
            if (averageQuality < 3)
                continue;

            updateTrustedQmersAndStatistics(statisticsState, read, qualities, i, kmer, correctnessProbability);
        }
        // Update counts for the rest of the read
        while (i < read.length()) {
            double quality = qualities.charAt(i) - 33;
            if (quality > 2) updateConditionalQualityProbability(statisticsState, read, i, quality);
            i++;
        }
    }

    private void writeSequenceReadToDb (final StatisticsState statisticsState,
                                        final int rowNum,
                                        final String read,
                                        final String qualities)
    {
        try {
            pushProcessedReadsToDB(rowNum, read, qualities, statisticsState);
        } catch ( SQLException e ) {
            e.printStackTrace();
            // streaming workers should never die. Bad programming practice.
            //System.exit(-1);
        }
    }

    private static void updateTrustedQmersAndStatistics (final StatisticsState statisticsState, final String read,
                                                         final String qualities, final int i, final String kmer,
                                                         final double correctnessProbability)
    {
        if (statisticsState.getBloomFilter().mightContain(kmer)) {
            double quality = qualities.charAt(i) - 33;
            if (quality > 2)
                updateConditionalQualityProbability(statisticsState, read, i, quality);

            if (statisticsState.getTrustedQmers().containsKey(kmer)) {
                statisticsState.getTrustedQmers().put(kmer,
                                                      statisticsState.getTrustedQmers().get(kmer) + 1D);
            } else {
                // This was already there in the bloom filter - count twice
                statisticsState.getTrustedQmers().put(kmer, 2D);
            }
        } else {
            statisticsState.getBloomFilter().put(kmer);
            // TODO: Is this assumption valid? What if errors have high correctness probability?
            // TODO: Now this is becoming philosophical. Go get a coffee!
            // TODO: If this is an SNP, the average quality must be high. So retain it.
            if (correctnessProbability > 0.999) {statisticsState.getTrustedQmers().put(kmer, 1.1);}
        }
    }

    private void pushProcessedReadsToDB (final int rowNum, final String read, final String qualities,
                                         final StatisticsState stats) throws
    SQLException
    {
        Map<String, Object> row = new Hashtable<String, Object>();
        row.put("rownum", rowNum);
        row.put("seqread", read);
        row.put("phred", qualities.replace("'", "''"));
        row.put("corrected", "");
        StatisticsState.insert(stats.getJdbcConnection(), row, StatisticsState.TABLE_NAME);
    }

    private static void updateConditionalQualityProbability (final StatisticsState statisticsState,
                                                             final String read,
                                                             final int ntIndex,
                                                             final double quality)
    {
        final char[] acgt = {'A', 'C', 'G', 'T'};
        final char positionalChar = read.charAt(ntIndex);
        double correctnessProbability = 1.0 - Math.pow(10.0, -0.1 * quality);

        statisticsState.positionalConditionalQualityCounts[ntIndex]
                [StatisticsState.getNucleotideIndex(positionalChar)]
                [StatisticsState.getNucleotideIndex(positionalChar)] += Math.log(correctnessProbability);
/*        statisticsState.positionalQualityCounts[ntIndex]
                [StatisticsState.getNucleotideIndex(positionalChar)] += correctnessProbability;*/

        for (char c : acgt)
            if (positionalChar != c) {
                statisticsState.positionalConditionalQualityCounts[ntIndex]
                        [StatisticsState.getNucleotideIndex(c)]
                        [StatisticsState.getNucleotideIndex(positionalChar)] += Math.log(
                        (1D - correctnessProbability) / 3D);  // GC content 50%, so divide the remaining prob among the other 3 bases
/*                statisticsState.positionalQualityCounts[ntIndex]
                        [StatisticsState.getNucleotideIndex(c)] += (1.0 - correctnessProbability) / 3.0;*/
            }
    }

    private static double getAverageQuality (final CharSequence qmer) {
        double averageQuality = 0D;
        for (int j = 0; j < qmer.length(); j++) {
            averageQuality += qmer.charAt(j) - 33;
        }
        return averageQuality / qmer.length();
    }
}
