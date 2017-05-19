package neu.sw.hxs.topology;

import neu.sw.hxs.bolt.CountBolt;
import neu.sw.hxs.bolt.RegexBolt;
import neu.sw.hxs.bolt.SocketBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;


public class KafkaLogLevelCountTopology {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if(args.length>6) {
            TopologyBuilder builder = new TopologyBuilder();
            KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                    .builder(args[0], args[1])
                    .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                    .setProp(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                    .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)
                    .setOffsetCommitPeriodMs(10_000)
                    .setFirstPollOffsetStrategy(EARLIEST)
                    .setMaxUncommittedOffsets(250)
                    .setGroupId(args[2])
                    .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                    .build();
            KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);
            HashMap<String, Object> hashMap = new HashMap<>();
            hashMap.put(RegexBolt.REGEX, args[3]);
            hashMap.put(RegexBolt.INDEX, args[4]);
            hashMap.put(RegexBolt.FIELD, "value");
            builder.setSpout("kafkaSpout", kafkaSpout, 4);
            builder.setBolt("regexBolt", new RegexBolt(), 4)
                    .addConfigurations(hashMap)
                    .shuffleGrouping("kafkaSpout");
            builder.setBolt("countBolt", new CountBolt(), 4)
                    .fieldsGrouping("regexBolt", new Fields("level"));
            HashMap<String, Object> stringObjectHashMap = new HashMap<>();
            stringObjectHashMap.put("ip", args[5]);
            stringObjectHashMap.put("port", args[6]);
            builder.setBolt("socketBolt", new SocketBolt(), 4)
                    .addConfigurations(stringObjectHashMap)
                    .shuffleGrouping("countBolt");

            Config conf = new Config();
            conf.setDebug(true);
            if (args.length > 7) {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopologyWithProgressBar(args[7], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("KafkaLogLevelCountTopology", conf, builder.createTopology());
            }
        }else {
            System.err.println("参数列表 [bootstrapServers] [topic] [group_id] [REGEX] [INDEX] [IP] [PORT] [name]");
        }
    }
}
