package level5;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import level2.BufferOperation;

/**
 * Created by dheeraj on 12/19/15.
 */
public class SumAggregator extends BaseOperation<SumAggregator.Context> implements Aggregator<SumAggregator.Context> {
    public SumAggregator(Fields fields) {
      //  super(fields);
    }

    public static class Context {
        Long value = 0l;
    }

    public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
        //get the group value for current grouping
        // TupleEntry tupleEntry = aggregatorCall.getGroup();
        Context context = new Context();
        aggregatorCall.setContext(context);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        TupleEntry tupleEntry = aggregatorCall.getArguments();
        Context context = aggregatorCall.getContext();
        context.value += tupleEntry.getLong("amount");
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {

        Context context = aggregatorCall.getContext();
        Tuple result = new Tuple();
        result.add(context.value);
        aggregatorCall.getOutputCollector().add(result);
    }
}
