package neu.sw.hxs.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class FileSpout extends BaseRichSpout{
    public static final String FILE_NAME = "fileName";
    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String fileName = (String) conf.get(FILE_NAME);
        try {
            bufferedReader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = collector;
    }

    public void nextTuple() {
        if (bufferedReader!=null){
            try {
                String log = bufferedReader.readLine();
                collector.emit(new Values(log));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }
}
