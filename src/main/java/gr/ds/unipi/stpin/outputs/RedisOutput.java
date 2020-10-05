package gr.ds.unipi.stpin.outputs;

import gr.ds.unipi.stpin.parsers.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.util.Pool;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisOutput implements Output<Record> {

    private static final Logger logger = LoggerFactory.getLogger(RedisOutput.class);
    private final Pool<Jedis> pool;
    private final Pipeline pipeline;
    private final int batchSize;
    private int count = 0;
    private final String database;

    public RedisOutput(String host, int port, String database, int batchSize) {
        this.pool =  RedisConnector.newRedisConnector(host, port, database).getPool();
        this.pipeline = pool.getResource().pipelined();
        this.batchSize = batchSize;
        this.database = database;
    }

    @Override
    public void out(Record record, String lineMetaData) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        String primaryKey = lineMetaData;

        Map<String,String> map = new HashMap<>();

        for (int i = 0; i < fieldNames.size(); i++) {

            if(fieldValues.get(i) instanceof Number){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                pipeline.zadd(database+":"+fieldNames.get(i),(double)fieldValues.get(i),primaryKey);

            }
            else if(fieldValues.get(i) instanceof String){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                pipeline.sadd(database+":"+fieldNames.get(i)+":"+fieldValues.get(i),primaryKey);
            }
            else if(fieldValues.get(i) instanceof Date){
                map.put(fieldNames.get(i),String.valueOf(((Date) fieldValues.get(i)).getTime()));
                pipeline.zadd(database+":"+fieldNames.get(i),(double)((Date) fieldValues.get(i)).getTime(),primaryKey);
            }
            else{
                try {
                    throw new Exception("");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        pipeline.sadd("primaryKey",primaryKey);
        pipeline.hset(primaryKey, map);

        count++;
        if(count==batchSize){
            logger.debug("Writing batch to Redis...");
            pipeline.sync();
            logger.debug("Done!");
            count=0;
        }

    }

    @Override
    public void close() {
        pipeline.sync();
        pipeline.close();
        pool.close();
    }
}
