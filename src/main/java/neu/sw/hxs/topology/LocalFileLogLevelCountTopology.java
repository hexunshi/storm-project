package neu.sw.hxs.topology;

import neu.sw.hxs.bolt.CountBolt;
import neu.sw.hxs.bolt.RegexBolt;
import neu.sw.hxs.bolt.SocketBolt;
import neu.sw.hxs.spout.FileSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;

public class LocalFileLogLevelCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        if (args.length > 4) {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("fileSpout", new FileSpout(), 1).addConfiguration(FileSpout.FILE_NAME, args[0]);
            HashMap<String, Object> hashMap = new HashMap<>();
            hashMap.put(RegexBolt.REGEX, args[1]);
            hashMap.put(RegexBolt.INDEX, args[2]);
            hashMap.put(RegexBolt.FIELD, "log");
            builder.setBolt("regexBolt", new RegexBolt(), 1).addConfigurations(hashMap).shuffleGrouping("fileSpout");
            builder.setBolt("countBolt", new CountBolt(), 4)
                    .fieldsGrouping("regexBolt", new Fields("level"));
            HashMap<String, Object> stringObjectHashMap = new HashMap<>();
            stringObjectHashMap.put("ip", args[3]);
            stringObjectHashMap.put("port", args[4]);
            builder.setBolt("socketBolt", new SocketBolt(), 1)
                    .addConfigurations(stringObjectHashMap)
                    .shuffleGrouping("countBolt");

            Config conf = new Config();
            conf.setDebug(true);
            if (args.length > 5) {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopologyWithProgressBar(args[5], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("LocalFileLogLevelCountTopology", conf, builder.createTopology());

                Thread.sleep(10000);

                cluster.shutdown();
            }
        } else {
            System.err.println("参数列表 [filePath] [REGEX] [INDEX] [IP] [PORT] [name]");
        }
    }
}
