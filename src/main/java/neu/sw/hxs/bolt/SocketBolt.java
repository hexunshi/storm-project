package neu.sw.hxs.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

public class SocketBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(SocketBolt.class);
    public static final String IP = "ip";
    public static final String PORT = "port";
    private Socket socket;
    private DataOutputStream dataOutputStream;
    private String ip;
    private int port;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        ip = (String) stormConf.get(IP);
        port = Integer.parseInt((String) stormConf.get(PORT));
        this.collector = collector;
        tryConnect();
    }

    @Override
    public void execute(Tuple input) {
        if (dataOutputStream == null || socket == null || socket.isClosed()) {
            collector.fail(input);
            tryConnect();
        } else {
            try {
                String result = "{" + input.getStringByField("level") + "," + input.getIntegerByField("count") + "}";
                dataOutputStream.writeUTF(result);
                collector.ack(input);
            } catch (IOException e) {
                collector.fail(input);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        super.cleanup();
        try {
            if (dataOutputStream != null) {
                dataOutputStream.flush();
                dataOutputStream.close();
            }
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void tryConnect() {
        try {
            socket = new Socket(ip, port);
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
