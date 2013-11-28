package project.cs439.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

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
        Hashtable<String, Double> incomingQmers = (Hashtable<String, Double>) objects.getValueByField("partialHistogram");
        double[][] positionalCounts = (double[][]) objects.getValueByField("positionalCounts");
        double[][][] positionalConditionalCounts = (double[][][]) objects.getValueByField("conditionalCounts");

        if (stats.size() == 0) {
            stats.add(incomingQmers);
            stats.add(positionalConditionalCounts);
            stats.add(positionalCounts);
        } else {
            // Updates stats by reference
            Hashtable<String, Double> reducedQmers = (Hashtable<String, Double>) stats.get(0);
            double[][][] pcc = (double[][][]) stats.get(1);
            double[][] pc = (double[][]) stats.get(2);

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
        return stats;
    }
}
