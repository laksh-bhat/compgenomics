package project.cs439.query;

import backtype.storm.tuple.Values;
import project.cs439.state.StatisticsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 11/22/13
 * Time: 1:46 AM
 */
public class statisticsQuery extends BaseQueryFunction<StatisticsState, List<Object>> {
    @Override
    public List<List<Object>> batchRetrieve (final StatisticsState abundanceHistogram, final List<TridentTuple> tridentTuples) {
        List<List<Object>> listList = new ArrayList<List<Object>>();
        for (int i = 0; i < tridentTuples.size(); i++){
            List<Object> list = new ArrayList<Object>(3);
            list.add(abundanceHistogram.getTrustedQmers());
            list.add(abundanceHistogram.positionalConditionalQualityCounts);
            list.add(abundanceHistogram.positionalQualityCounts);
            listList.add(list);
        }
        return listList;
    }

    @Override
    public void execute (final TridentTuple objects, final List<Object> list, final TridentCollector collector) {
        System.out.println("Querying State -- List " + list);
        if(list != null) 
            collector.emit(new Values(list.get(0), list.get(1), list.get(2)));
        else
            collector.emit(new Values(null, null, null));
    }
}
