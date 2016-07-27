package level4;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * Created by dheeraj on 12/18/15.
 */
public class wordCount {
    public static FlowDef wordCount(Tap source, Tap sink) {
        Pipe pipe = new Pipe("pipe");
        // this is the simple method for word count.
      //  pipe = new CountBy(pipe, new Fields("word"), new Fields("count"));

        RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields("word"), "[ \\[\\]\\(\\),.]" );
        pipe=new Each(pipe,splitter);

        pipe = new GroupBy(pipe, new Fields("word") );
        pipe = new Every(pipe, new Count());

        return FlowDef.flowDef()
                .addSource(pipe, source)
                .addTail(pipe)
                .addSink(pipe, sink);
    }
}
