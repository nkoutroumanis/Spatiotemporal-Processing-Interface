package gr.ds.unipi.stpin.outputs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.Pool;

public class RedisConnector {

    private final Pool<Jedis> pool;
    private final String database;

    private RedisConnector(String host, int port, String database) {
        pool = new JedisPool(new GenericObjectPoolConfig(), host, port, 2000, 2000, null, 0, null, false, null, null, null);
        this.database = database;
    }

    public static RedisConnector newRedisConnector(String host, int port, String database) {
        return new RedisConnector(host, port, database);
    }

    public String getDatabase() {
        return database;
    }

    public Pool<Jedis> getPool() {
        return pool;
    }

}
