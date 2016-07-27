package level5;

import cascading.flow.FlowDef;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by dheeraj on 12/19/15.
 */
public class AggregatorAssembly {
    public static FlowDef AggregatorOperation(Tap sourceTap, Tap sinkTap) {
        Pipe aggPipe = new Pipe("aggregator");

        aggPipe = new GroupBy(aggPipe, new Fields("id", "name"));

        aggPipe = new Every(aggPipe, new SumAggregator(new Fields("id", "name", "amount")),Fields.ALL);
        return FlowDef.flowDef()
                .addSource(aggPipe, sourceTap)
                .addSink(aggPipe, sinkTap)
                .addTail(aggPipe);
    }
}
