package level2;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by dheeraj on 12/18/15.
 */
public class bufferOperate extends BaseOperation<TupleEntry> implements Buffer<TupleEntry> {

    public bufferOperate(Fields comparables) {

    }

    public void operate(FlowProcess flowProcess, BufferCall<TupleEntry> bufferCall) {
        // get the group values for the current grouping
        TupleEntry group = bufferCall.getGroup();

        // get the current argument values for this grouping
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        String temp = new String();

        // create a tuple to hold our result values
        Tuple result = new Tuple();
        while (arguments.hasNext()) {
            TupleEntry argument = arguments.next();
            temp += argument.getString("value")+"\t";

        }
        // add the string into the tuple result.
        result.add(temp);
        bufferCall.getOutputCollector().add(result);
    }
}
