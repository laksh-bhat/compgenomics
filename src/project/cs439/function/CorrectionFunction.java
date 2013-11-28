package project.cs439.function;

import project.cs439.state.StatisticsState;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 11/24/13
 * Time: 9:49 PM
 */
public class CorrectionFunction implements Function {
    private int localPartition, noOfPartitions;
    final static char[] acgt = {'A', 'C', 'G', 'T'};

    @Override
    public void execute (final TridentTuple tuple, final TridentCollector collector) {
        System.out.println("Debug: Started correction... ");
        ResultSet resultSet = null;
        Connection dbConnection = null;
        //double[][] positionalCounts = (double[][]) tuple.getValueByField("positionalCounts");
        // These are not real probabilities. We need to normalize them. But that's unnecessary for our purposes.
        double[][][] conditionalProbs = (double[][][]) tuple.getValueByField("conditionalCounts");
        Map<String, Double> trustedQmers = (Hashtable<String, Double>) tuple.getValueByField("histogram");

        guessMoreUntrustedQmers(trustedQmers, 4, conditionalProbs);

        try {
            dbConnection = StatisticsState.getNewDatabaseConnection();
            int totalReadsBeingProcessed = StatisticsState.getMaxReadId(dbConnection, StatisticsState.TABLE_NAME);
            int totalReadsProcessedByCurrentPartition = totalReadsBeingProcessed / noOfPartitions;
            int startReadIndex = localPartition * totalReadsProcessedByCurrentPartition + 1;
            int endReadIndex = totalReadsBeingProcessed;
            if (noOfPartitions != localPartition + 1)
                endReadIndex = (localPartition + 1) * totalReadsProcessedByCurrentPartition;
            resultSet = StatisticsState.getAll(dbConnection, StatisticsState.TABLE_NAME, startReadIndex, endReadIndex);

            while (resultSet.next()) {
                int rowNum = resultSet.getInt("rownum");
                CharSequence seqRead = resultSet.getString("seqread");
                CharSequence qualities = resultSet.getString("phred");

                List<List<Integer>> untrustedRegions = findUntrustedIntersections(seqRead, qualities, trustedQmers, StatisticsState.k);
                for (List<Integer> region : untrustedRegions) {
                    int start = region.get(0), end = region.get(1);
                    if (end - start == StatisticsState.k && end != seqRead.length()) { // case1, size of the region is k
                        seqRead = correctSingleNucleotideError(conditionalProbs, trustedQmers, seqRead, start, end);
                    } else {  // case 2 longer region.
                        seqRead = correctMultipleErrorsIfYouCan(conditionalProbs, trustedQmers, seqRead, start, end);
                    }
                }
                StatisticsState.updateCorrections(dbConnection, StatisticsState.TABLE_NAME, seqRead.toString(), rowNum);
            }
        } catch ( SQLException e ) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (dbConnection != null) dbConnection.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param conditionalProbs
     * @param trustedQmers
     * @param seqRead
     * @param start
     * @param end
     * @return
     */
    private static CharSequence correctMultipleErrorsIfYouCan (final double[][][] conditionalProbs,
                                                               final Map<String, Double> trustedQmers,
                                                               CharSequence seqRead, final int start, final int end)
    {// exponential problem. 2 cases. region reaches the end of read or not.
        // Obs: In latter case, the last nucleotide of the last trusted k-mer must belong to trusted region
        if (end == seqRead.length()) {
            StringBuilder sb = new StringBuilder().append(seqRead);
            correctBorderUntrustedKmers(conditionalProbs, trustedQmers, seqRead, start, end, sb);
            seqRead = sb.toString();
        } else {
            // TODO this is suspect: revisit
            StringBuilder sb = correctMultipleErrorsStrictlyInsideRead(conditionalProbs, trustedQmers, seqRead, start, end);
            seqRead = sb.toString();
        }
        return seqRead;
    }

    /**
     * @param conditionalProbs
     * @param trustedQmers
     * @param seqRead
     * @param start
     * @param end
     * @return
     */
    private static StringBuilder correctMultipleErrorsStrictlyInsideRead (final double[][][] conditionalProbs,
                                                                          final Map<String, Double> trustedQmers,
                                                                          CharSequence seqRead,
                                                                          final int start,
                                                                          final int end)
    {   // continuously divide into k k-mer batches and correct the first nt of the last one in the batch
        int kRegionStart = start;
        while (kRegionStart + StatisticsState.k <= end) {
            seqRead = correctSingleNucleotideError(conditionalProbs, trustedQmers, seqRead,
                                                   kRegionStart,
                                                   kRegionStart + StatisticsState.k - 1 /*last untrusted kmer in this batch*/);
            kRegionStart += StatisticsState.k;
        }
        TreeMap<Double, Character> corrections = new TreeMap<Double, Character>();
        searchNucleotideWithHighestLikelihood(conditionalProbs, trustedQmers, seqRead, kRegionStart, end,
                                              corrections);
        StringBuilder sb = new StringBuilder();
        sb.append(seqRead).setCharAt(end,
                                     corrections.size() > 0 ? corrections.lastEntry().getValue()
                                                            : seqRead.charAt(end));
        return sb;
    }

    /**
     * @param conditionalProbs
     * @param trustedQmers
     * @param seqRead
     * @param start
     * @param end
     * @param sb
     */
    public static void correctBorderUntrustedKmers (final double[][][] conditionalProbs,
                                                    final Map<String, Double> trustedQmers,
                                                    final CharSequence seqRead,
                                                    final int start, final int end, final StringBuilder sb)
    {
        int replaceMe = start + StatisticsState.k - 1; // why? see comment in caller
        int lastIndex = seqRead.length();

        TreeMap<Double, Character> corrections = new TreeMap<Double, Character>();
        TreeMap<Integer, Character> majority = new TreeMap<Integer, Character>();

        // We have now reduced the search space, try all combinations
        for (char nt : acgt) {
            if (nt == seqRead.charAt(replaceMe)) continue;
            int corrected = 0;
            sb.setCharAt(replaceMe, nt);
            // Test if all k-mer become trusted
            for (int i = start; i + StatisticsState.k <= seqRead.length(); i++)
                if (trustedQmers.containsKey(sb.substring(i, i + StatisticsState.k)))
                    corrected++;

            if (corrected == lastIndex - replaceMe)
                corrections.put(conditionalProbs[replaceMe]
                                        [StatisticsState.getNucleotideIndex(nt)]
                                        [StatisticsState.getNucleotideIndex(seqRead.charAt(replaceMe))], nt);
            else
                majority.put(corrected, nt);
        }
        if (corrections.size() == 0) {  // None of the ACTG corrected everything, go greedy
            // that obviously didn't work
            // If we at least correct the majority, give up.
            if (majority.lastKey() == 1) {
                sb.setCharAt(replaceMe, majority.get(majority.lastKey()));
                // There must be more errors starting at the next k-mer; scavenger hunt!
                correctBorderUntrustedKmers(conditionalProbs, trustedQmers, seqRead, start + 1, end, sb);
            } else if (majority.lastKey() >= (end - replaceMe) / 2)
                sb.setCharAt(replaceMe, majority.get(majority.lastKey()));
            else {
                // We fail to correct; Set it back to the original
                sb.setCharAt(replaceMe, seqRead.charAt(replaceMe));
            }
        } else {
            sb.setCharAt(replaceMe, corrections.lastEntry().getValue());
        }
    }

    /**
     * @param observedNucleotide
     * @param corrections
     * @param overlapRegion
     * @param end
     * @param trustedQmers
     * @param conditionalProbabilities
     */
    private static void addToValidCorrectionsIfCertified (final char observedNucleotide,
                                                          final Map<Double, Character> corrections,
                                                          final String overlapRegion,
                                                          final int end,
                                                          final Map<String, Double> trustedQmers,
                                                          final double[][][] conditionalProbabilities)
    {
        if (trustedQmers.containsKey(overlapRegion.substring(0, StatisticsState.k)) &&
                trustedQmers.containsKey(
                        overlapRegion.substring(overlapRegion.length() - StatisticsState.k, overlapRegion.length()))) {
            // compute likelihood and add to corrections map
            corrections.put
                    (
                            conditionalProbabilities
                                    [end]
                                    [StatisticsState.getNucleotideIndex(overlapRegion.charAt(StatisticsState.k - 1))]
                                    [StatisticsState.getNucleotideIndex(observedNucleotide)],
                            overlapRegion.charAt(StatisticsState.k - 1)
                    )
            ;
        }
    }

    /**
     * @param conditionalCounts
     * @param trustedQmers
     * @param seqRead
     * @param start
     * @param end
     * @return
     */
    public static CharSequence correctSingleNucleotideError (final double[][][] conditionalCounts,
                                                             final Map<String, Double> trustedQmers,
                                                             final CharSequence seqRead,
                                                             final int start,
                                                             final int end)
    {

        TreeMap<Double, Character> corrections = new TreeMap<Double, Character>();
        searchNucleotideWithHighestLikelihood(conditionalCounts, trustedQmers, seqRead, start, end, corrections);
        StringBuilder sb = new StringBuilder();
        char nt = corrections.size() > 0 ? corrections.lastEntry().getValue() : seqRead.charAt(end);
        sb.append(seqRead).setCharAt(end, nt);
        return sb.toString();
    }

    /**
     * @param conditionalCounts
     * @param trustedQmers
     * @param seqRead
     * @param start
     * @param end
     * @param corrections
     */
    private static void searchNucleotideWithHighestLikelihood (final double[][][] conditionalCounts,
                                                               final Map<String, Double> trustedQmers,
                                                               final CharSequence seqRead,
                                                               final int start,
                                                               final int end,
                                                               final TreeMap<Double, Character> corrections)
    {
        for (char nt : acgt) {
            if (seqRead.charAt(end) != nt) {
                StringBuilder sb = new StringBuilder();
                // extract k-mer and replace last character
                sb.append(seqRead).setCharAt(end, nt);
                addToValidCorrectionsIfCertified(seqRead.charAt(end), corrections, sb.substring(start, end + StatisticsState.k), end, trustedQmers, conditionalCounts);
            }
        }
    }

    /**
     * @param trustedQmers
     * @param cutoff
     * @param conditionalCounts
     */
    private static void guessMoreUntrustedQmers (final Map<String, Double> trustedQmers, double cutoff,
                                                 final double[][][] conditionalCounts)
    {
        for (Iterator<String> iterator = trustedQmers.keySet().iterator(); iterator.hasNext(); ) {
            final String qMer = iterator.next();
            double multiplicity = trustedQmers.get(qMer);
            // TODO We need to reduce counts in our conditionalCounts 3-D array
            if (multiplicity < cutoff) iterator.remove();
        }
    }

    /**
     * @param seqRead
     * @param phred
     * @param trustedQmers
     * @param k
     * @return
     */
    private static List<List<Integer>> findUntrustedIntersections (final CharSequence seqRead,
                                                                   final CharSequence phred,
                                                                   final Map<String, Double> trustedQmers,
                                                                   int k)
    {
        List<List<Integer>> untrustedRanges = new ArrayList<List<Integer>>(1);
        List<Integer> untrustedQmerRange = new ArrayList<Integer>();
        boolean isUntrustedRegionBegun = false;
        for (int i = 0; i + k < seqRead.length(); i++) {
            if (getAverageQuality(phred.subSequence(i, i + k)) < 3)
                continue;

            CharSequence qMer = seqRead.subSequence(i, i + k);
            if (isUntrusted(trustedQmers, qMer)) {
                if (!isUntrustedRegionBegun) {
                    untrustedQmerRange.add(i);
                    isUntrustedRegionBegun = true;
                }
            } else if (isUntrustedRegionBegun) {
                if (i + k == seqRead.length() - 1) untrustedQmerRange.add(seqRead.length());
                else untrustedQmerRange.add(i - 1);

                untrustedRanges.add(untrustedQmerRange);
                isUntrustedRegionBegun = false;
                untrustedQmerRange = new ArrayList<Integer>();
            }
        }
        return untrustedRanges;
    }

    private static boolean isUntrusted (final Map<String, Double> trustedQmers, final CharSequence qMer) {
        return !trustedQmers.containsKey(qMer);
    }

    private static double qToP (double q) {
        return 1.0 - Math.pow(10.0, -0.1 * q);
    }

    private static double getAverageQuality (final CharSequence qmer) {
        double averageQuality = 0D;
        for (int j = 0; j < qmer.length(); j++) {
            averageQuality += qmer.charAt(j) - 33;
        }
        return averageQuality / qmer.length();
    }

    @Override
    public void prepare (final Map map, final TridentOperationContext operationContext) {
        this.localPartition = operationContext.getPartitionIndex();
        this.noOfPartitions = operationContext.numPartitions();
    }

    @Override
    public void cleanup () {}
}
