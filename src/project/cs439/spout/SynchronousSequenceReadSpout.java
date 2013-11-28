package project.cs439.spout;

/**
 * User: lbhat@damsl
 * Date: 11/23/13
 * Time: 5:04 PM
 */

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

@SuppressWarnings ({"serial", "rawtypes"})
public class SynchronousSequenceReadSpout implements IRichSpout {
    public SynchronousSequenceReadSpout (SequenceReadStreamingSpout spout) {
        this.done = false;
        baseSpout = spout;
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(baseSpout.fieldNames));
    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
        return new Config();
    }

    @Override
    public void open (Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.err.println("Debug: Opening SequenceReadStreamingSpout Instance...");
        _collector = collector;
        try {
            scanner = new Scanner(new File(baseSpout.fileName));
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close () {
        scanner.close();
        done = true;
    }

    @Override
    public void activate () {
        if (scanner != null)
            scanner.close();
        scanner = new Scanner(baseSpout.fileName);
    }

    @Override
    public void deactivate () {
        scanner.close();
    }

    @Override
    public void nextTuple () {
        try {
            if (!baseSpout.isDone()) {
                Thread.sleep(1000);
                return;
            }
        } catch ( InterruptedException ignore ) {}

        int i = 0;
        String[] fastQ = {null, null};

        while (scanner.hasNextLine() && i < 4) {
            String fastqLine = scanner.nextLine();
            if (i == READ_INDEX) {
                fastQ[0] = fastqLine;
            } else if (i == PHRED_INDEX) {
                fastQ[1] = fastqLine;
            }
            i++;
        }
        if (fastQ[0] != null && fastQ[1] != null) {
            _collector.emit(new Values(rowCount++, fastQ));
        } else { done = true; }
    }

    @Override
    public void ack (Object msgId) {}

    @Override
    public void fail (Object msgId) {}

    public boolean isDone () {
        return done;
    }

    private boolean done;
    private int     rowCount;
    private Scanner scanner;
    SpoutOutputCollector       _collector;
    SequenceReadStreamingSpout baseSpout;
    private static final int READ_INDEX = 1, PHRED_INDEX = 3;
}
