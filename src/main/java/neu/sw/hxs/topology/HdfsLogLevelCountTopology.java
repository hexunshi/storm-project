package neu.sw.hxs.topology;

import neu.sw.hxs.bolt.CountBolt;
import neu.sw.hxs.bolt.RegexBolt;
import neu.sw.hxs.bolt.SocketBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;


public class HdfsLogLevelCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        if (args.length > 6) {
            System.setProperty("HADOOP_USER_NAME", "root");
            TopologyBuilder builder = new TopologyBuilder();
            HdfsSpout hdfsSpout = new HdfsSpout().setReaderType("text")
                    .withOutputFields(TextFileReader.defaultFields)
                    .setHdfsUri(args[0])
                    .setSourceDir(args[1])
                    .setArchiveDir(args[2])
                    .setBadFilesDir(args[3]);
            HashMap<String, Object> hashMap = new HashMap<>();
            hashMap.put(RegexBolt.REGEX, args[4]);
            hashMap.put(RegexBolt.INDEX, args[5]);
            hashMap.put(RegexBolt.FIELD, "line");

            builder.setSpout("hdfsSpout", hdfsSpout, 1);
            builder.setBolt("regexBolt", new RegexBolt(), 1)
                    .addConfigurations(hashMap).shuffleGrouping("hdfsSpout");
            builder.setBolt("countBolt", new CountBolt(), 1)
                    .fieldsGrouping("regexBolt", new Fields("level"));
            HashMap<String, Object> stringObjectHashMap = new HashMap<>();
            stringObjectHashMap.put("ip", args[6]);
            stringObjectHashMap.put("port", args[7]);
            builder.setBolt("socketBolt", new SocketBolt(), 4)
                    .addConfigurations(stringObjectHashMap)
                    .shuffleGrouping("countBolt");

            Config conf = new Config();
            conf.setDebug(true);

            if (args.length > 8) {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopologyWithProgressBar(args[8], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(1);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("hdfsLogLevelCountTopology", conf, builder.createTopology());
                Thread.sleep(90000);
                cluster.shutdown();
            }
        } else {
            System.err.println("参数列表 [HdfsUri] [SourceDir] [ArchiveDir] [BadFilesDir] [REGEX] [INDEX] [IP] [PORT] [name]");
        }
    }
}
