package level5;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 * Created by dheeraj on 12/19/15.
 */
public class TestAggregator {

    @Test
    public void aggregatorTest() {
        //source path and tap
        String sourcePath = "src/test/resources/level5.txt";
        Tap sourceTap = new FileTap(new TextDelimited(new Fields("id", "name", "amount"), ";"), sourcePath);

        String sinkPath = "src/test/resources/outputLevel5.txt";
        Tap sinkTap = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.KEEP);

        FlowDef flowDef=AggregatorAssembly.AggregatorOperation(sourceTap, sinkTap);
        new LocalFlowConnector().connect(flowDef).complete();

    }

}
