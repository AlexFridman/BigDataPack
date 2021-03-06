package level4;

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
 * Created by dheeraj on 12/18/15.
 */
public class TestWordCount {
       @Test
    public void wordCount(){
        //input of the job
        String sourcepath= "src/test/resources/Outlevel3.txt";
        Tap source = new FileTap(new TextLine(new Fields("word")),sourcepath);

        //output of the job
        String sinkpath = "src/test/resources/Outlevel4.txt";
        Tap sink = new FileTap(new TextDelimited(true, "\t"), sinkpath, SinkMode.REPLACE );

        //Run the job
        FlowDef flowDef = wordCount.wordCount(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
