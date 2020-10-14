package gr.ds.unipi.stpin.outputs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.Pool;

import java.io.IOException;
import java.util.Objects;

public class HBaseConnector {

    private final Connection connection;

    private HBaseConnector(String host) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", host);
        connection = ConnectionFactory.createConnection(config);
    }

    public Connection getConnection(){
        return connection;
    }

    public static HBaseConnector newHBaseConnector(String host){
        try {
            return new HBaseConnector(host);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
