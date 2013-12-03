package project.cs439.query;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 11/22/13
 * Time: 2:00 AM
 */
public class StatisticsCombiner implements CombinerAggregator<List<Object>> {
    @Override
    public List<Object> init (final TridentTuple tuple) {
        List<Object> initial = new ArrayList<Object>(1);
        initial.add(tuple.getValueByField("conditionalCounts"));
        initial.add(tuple.getValueByField("positionalCounts"));
        return initial;
    }

    @Override
    public List<Object> combine (final List<Object> totalCounts, final List<Object> localCounts) {
        if (localCounts == null) return totalCounts;
        else if (totalCounts == null) return localCounts;

        double[][][] pcc = (double[][][]) totalCounts.get(0);
        double[][] pc = (double[][]) totalCounts.get(1);

        double[][][] lpcc = (double[][][]) localCounts.get(0);
        double[][] lpc = (double[][]) localCounts.get(1);

        // combine conditional counts
        for (int i = 0; i < pcc.length; i++)
            for (int j = 0; j < pcc[i].length; j++)
                for (int k = 0; k < pcc[i][j].length; k++)
                    pcc[i][j][k] += lpcc[i][j][k];

        // combine positional counts
        for (int i = 0; i < pc.length; i++)
            for (int j = 0; j < pc[i].length; j++)
                pc[i][j] += lpc[i][j];

        System.out.println("Debug: Finished Combining. ");
        return totalCounts;
    }

    @Override
    public List<Object> zero () {
        return null;
    }
}
