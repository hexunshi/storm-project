package neu.sw.hxs.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexBolt implements IRichBolt {
    public static final String REGEX = "regex";
    public static final String INDEX = "index";
    public static final String FIELD = "field";
    int index;
    String field;
    Pattern regex;
    OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String regexString = (String) stormConf.get(REGEX);
        this.collector = collector;
        this.field = (String) stormConf.get(FIELD);
        this.index = Integer.parseInt((String) stormConf.get(INDEX));
        this.regex = Pattern.compile(regexString);
    }

    public void execute(Tuple input) {
        String log = input.getStringByField(field);
        if (log != null) {
            Matcher matcher = regex.matcher(log);
            if (matcher.find()) {
                if (matcher.groupCount() > index) {
                    String level = matcher.group(index);
                    System.err.println(level+log);
                    collector.emit(new Values(level));
                } else {
                    System.err.println("不符合规格丢弃");
                }
            } else {
                System.err.println("不符合规格丢弃" + log);
            }
        }
        collector.ack(input);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("level"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
