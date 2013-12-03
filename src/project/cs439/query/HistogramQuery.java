package project.cs439.query;

import backtype.storm.tuple.Values;
import project.cs439.state.StatisticsState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/3/13
 * Time: 10:40 AM
 */
public class HistogramQuery implements QueryFunction<StatisticsState.Histogram, Map<String, Double>> {
    @Override
    public List<Map<String , Double>> batchRetrieve (final StatisticsState.Histogram histogram, final List<TridentTuple> tridentTuples) {
        List<Map<String, Double>> listMap = new ArrayList<Map<String, Double>>(1);
        for (int i = 0; i < tridentTuples.size(); i++) {
            listMap.add(histogram.getTrustedQmers());
        }
        return listMap;
    }

    @Override
    public void execute (final TridentTuple tuple,
                         final Map<String, Double> partialHistogram,
                         final TridentCollector collector)
    {
        collector.emit(new Values(partialHistogram));
    }


    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex();
        noOfPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup () {}

    private int localPartition;
    private int noOfPartitions;
}
