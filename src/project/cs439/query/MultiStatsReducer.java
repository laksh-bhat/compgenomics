package project.cs439.query;

import backtype.storm.tuple.Values;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: lbhat@damsl
 * Date: 12/3/13
 * Time: 12:26 PM
 */
public class MultiStatsReducer implements MultiReducer<Map<String, Object>> {

    @Override
    public void prepare (final Map map, final TridentMultiReducerContext tridentMultiReducerContext) {}

    @Override
    public Map<String, Object> init (final TridentCollector tridentCollector) {
        return new ConcurrentHashMap<String, Object>();
    }

    @Override
    public void execute (final Map<String, Object> reducedMap,
                         final int streamIndex,
                         final TridentTuple tuple,
                         final TridentCollector collector)
    {
        switch (streamIndex){
            case 0 : reducedMap.put("histogram", tuple.getValueByField("histogram"));
                break;
            case 1:
                List<Object> stats = (List<Object>) tuple.getValueByField("statistics");
                reducedMap.put("conditionalCounts", stats.get(0));
                reducedMap.put("positionalCounts", stats.get(1));
                break;
        }
    }

    @Override
    public void complete (final Map<String, Object> reducedMap, final TridentCollector collector) {
        collector.emit(new Values(reducedMap.get("histogram"), reducedMap.get("conditionalCounts"), reducedMap.get("positionalCounts")));
    }

    @Override
    public void cleanup () {}
}
