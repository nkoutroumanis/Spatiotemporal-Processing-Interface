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

public class RedisInstanceOutput implements RedisOutput {

    private static final Logger logger = LoggerFactory.getLogger(RedisOutput.class);
    private final Pool<Jedis> pool;
    private final Pipeline pipeline;
    private final int batchSize;
    private int count = 0;
    private final String database;

    private final boolean indexes;
    private final boolean spatialIndex;
    private final boolean spatiotemporalIndex;

    public RedisInstanceOutput(String host, int port, String database, int batchSize, boolean indexes, boolean spatialIndex, boolean spatiotemporalIndex) {
        this.pool =  RedisConnector.newRedisConnector(host, port, database).getPool();
        this.pipeline = pool.getResource().pipelined();
        this.batchSize = batchSize;
        this.database = database;
        this.indexes = indexes;
        this.spatialIndex = spatialIndex;
        this.spatiotemporalIndex = spatiotemporalIndex;
    }

    @Override
    public void out(Record record, String lineMetaData) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        String primaryKey = RandomGenerator.randomCharacterNumericString();

        Map<String,String> map = new HashMap<>();

        String dateFieldName = "";

        for (int i = 0; i < fieldNames.size(); i++) {

            if(fieldValues.get(i) instanceof Number){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                if(indexes) {
                    pipeline.zadd(database+":"+fieldNames.get(i),(double)fieldValues.get(i),primaryKey);
                }
            }
            else if(fieldValues.get(i) instanceof String){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                if(indexes) {
                    pipeline.sadd(database+":"+fieldNames.get(i)+":"+fieldValues.get(i),primaryKey);
                }
            }
            else if(fieldValues.get(i) instanceof Date){
                dateFieldName = fieldNames.get(i);
                map.put(fieldNames.get(i),String.valueOf(((Date) fieldValues.get(i)).getTime()));
                if(indexes){
                    pipeline.zadd(database+":"+fieldNames.get(i),(double)((Date) fieldValues.get(i)).getTime(),primaryKey);
                }
            }
            else if(fieldValues.get(i)==null){
                map.put(fieldNames.get(i),"Null");
                if(indexes) {
                    pipeline.sadd(database+":"+fieldNames.get(i)+":"+"Null",primaryKey);
                }
            }
            else{
                try {
                    throw new Exception("");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        String[] lineMetaDataArray = lineMetaData.split(":");

        if(spatialIndex){
            pipeline.zadd(database+":"+"location",Long.parseLong(lineMetaDataArray[0]), primaryKey);
        }
        if(spatiotemporalIndex){
            pipeline.zadd(database+":"+"location:"+dateFieldName,Long.parseLong(lineMetaDataArray[1]), primaryKey);
        }
        pipeline.sadd(database+":"+"primaryKeys",primaryKey);
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

    public boolean hasSpatialIndex() {
        return spatialIndex;
    }
}
