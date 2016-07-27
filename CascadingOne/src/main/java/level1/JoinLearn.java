package level1;

import cascading.flow.FlowDef;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by dheeraj on 12/18/15.
 */
public class JoinLearn {
    public static FlowDef JoinFiles(Tap source1Tap, Tap source2Tap, Tap sinkTap) {
        // create a left pipe and right pipe before joining.
        Pipe leftPipe = new Pipe("leftPipe");
        Pipe rightPipe = new Pipe("rightPipe");

        //  rename the common fields before using CoGroup.
        rightPipe = new Rename(rightPipe, new Fields("id"), new Fields("s_id"));

        // output pipe after join
        Pipe jointPipe ;

        // In this level you do leftJoin but there are other option like rightJoin and InnerJoin just like in SQLJoin,
        jointPipe = new CoGroup(leftPipe, new Fields("id"), rightPipe, new Fields("s_id"),new LeftJoin());
        return FlowDef.flowDef()
                .addSource(leftPipe, source1Tap)
                .addSource(rightPipe, source2Tap)
                .addTail(jointPipe)
                .addSink(jointPipe, sinkTap);

    }
}
