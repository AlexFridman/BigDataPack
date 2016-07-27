package level4;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;


/**
 * hadoop job for wordcount
 * using cascading // test can be run using level4 test case
 * Created by dheeraj on 12/20/15.
 */
public class Main {
    public static void main(String args[]) {
        String docPath = args[0];
        String wcPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        // create source and sink taps
        Tap docTap = new Hfs(new TextDelimited(new Fields("word")), docPath);
        Tap wcTap = new Hfs(new TextDelimited(true, "\t"), wcPath);

        FlowDef flowDef = wordCount.wordCount(docTap, wcTap);

        // write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect(flowDef);
        wcFlow.writeDOT("dot/wc.dot");
        wcFlow.complete();

    }
}
