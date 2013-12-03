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
public class HistogramCombiner implements CombinerAggregator<HashMap<String, Double>> {
    @Override
    public HashMap<String, Double> init (final TridentTuple tuple) {
         return (HashMap<String, Double>) tuple.getValueByField("partialHistogram");
    }

    @Override
    public HashMap<String, Double> combine (final HashMap<String, Double> collectorHistogram,
                                            final HashMap<String, Double> partialHistogram)
    {
        for (String key : collectorHistogram.keySet()){
            if (partialHistogram.containsKey(key))
                collectorHistogram.put(key, partialHistogram.get(key) + collectorHistogram.get(key));
            partialHistogram.remove(key);
        }
        collectorHistogram.putAll(partialHistogram);
        return collectorHistogram;
    }

    @Override
    public HashMap<String, Double> zero () {
        return new HashMap<String, Double>(0);
    }
}
