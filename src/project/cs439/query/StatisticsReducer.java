package project.cs439.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 11/22/13
 * Time: 2:00 AM
 */
public class StatisticsReducer implements ReducerAggregator<List<Object>> {
    @Override
    public List<Object> init () {
        return new ArrayList<Object>();
    }

    @Override
    public List<Object> reduce (final List<Object> stats, final TridentTuple objects) {
        Map<String, Double> incomingQmers = (Map<String, Double>) objects.getValueByField("partialHistogram");
        double[][] positionalCounts = (double[][]) objects.getValueByField("positionalCounts");
        double[][][] positionalConditionalCounts = (double[][][]) objects.getValueByField("conditionalCounts");
        doReduction(stats, incomingQmers, positionalCounts, positionalConditionalCounts);
        return stats;
    }

    private void doReduction (final List<Object> stats,
                              final Map<String, Double> incomingQmers,
                              final double[][] positionalCounts, final double[][][] positionalConditionalCounts)
    {
        if (stats.size() == 0) {
            System.out.println(MessageFormat.format("Debug: Initial Reduce. Incoming Qmer Size {0}." +
                                                            " TrustedQmers size {1}.", incomingQmers.size(), incomingQmers.size()));
            stats.add(incomingQmers);
            stats.add(positionalConditionalCounts);
            stats.add(positionalCounts);
        } else {
            // Updates stats by reference
            Map<String, Double> reducedQmers = (Map<String, Double>) stats.get(0);
            double[][][] pcc = (double[][][]) stats.get(1);
            double[][] pc = (double[][]) stats.get(2);

            System.out.println(MessageFormat.format("Debug: Complex Reduce. Incoming Qmer Size {0}." +
                                                            " TrustedQmers size {1}.", incomingQmers.size(), reducedQmers.size()));

            // Reduce conditional counts
            for (int i = 0; i < positionalConditionalCounts.length; i++)
                for (int j = 0; j < positionalConditionalCounts[i].length; j++)
                    for (int k = 0; k < positionalConditionalCounts[i][j].length; k++)
                        pcc[i][j][k] += positionalConditionalCounts[i][j][k];

            // Reduce positional counts
            for (int i = 0; i < positionalCounts.length; i++)
                for (int j = 0; j < positionalCounts[i].length; j++)
                    pc[i][j] += positionalCounts[i][j];

            // Reduce trusted Qmer map
            for (String qmer : incomingQmers.keySet()) {
                if (reducedQmers.containsKey(qmer))
                    reducedQmers.put(qmer, reducedQmers.get(qmer) + incomingQmers.get(qmer));
                else
                    reducedQmers.put(qmer, incomingQmers.get(qmer));
            }
        }
        System.out.println("Debug: Finished Reducing. ");
    }
}
