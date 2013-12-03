package project.cs439.query;

import storm.trident.operation.*;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 11/22/13
 * Time: 2:00 AM
 */
public class HistogramCombiner implements CombinerAggregator<Map<String, Double>> {
    @Override
    public Map<String, Double> init (final TridentTuple tuple) {
         return (Map<String, Double>) tuple.getValueByField("partialHistogram");
    }

    @Override
    public Map<String, Double> combine (final Map<String, Double> collectorHistogram,
                                            final Map<String, Double> partialHistogram)
    {
        for (String key : collectorHistogram.keySet()){
            if (partialHistogram.containsKey(key))
                collectorHistogram.put(key, partialHistogram.get(key) + collectorHistogram.get(key));
            partialHistogram.remove(key);
        }
        collectorHistogram.putAll(partialHistogram);
	System.out.println("Debug: HistogramCombiner finished with histogram size " + collectorHistogram.size());
        return collectorHistogram;
    }

    @Override
    public Map<String, Double> zero () {
        return (Map<String, Double>) new HashMap<String, Double>(0);
    }
}
