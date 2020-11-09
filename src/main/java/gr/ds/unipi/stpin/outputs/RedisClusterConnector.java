package gr.ds.unipi.stpin.outputs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

public class RedisClusterConnector {

    private final JedisCluster jedisCluster;

    private RedisClusterConnector(Set<HostAndPort> nodes) {
        this.jedisCluster = new JedisCluster (nodes,0);
    }

    public static RedisClusterConnector newRedisClusterConnector(Set<HostAndPort> nodes) {
        return new RedisClusterConnector(nodes);
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

}
