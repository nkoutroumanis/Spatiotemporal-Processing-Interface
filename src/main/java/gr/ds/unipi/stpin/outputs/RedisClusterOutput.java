package gr.ds.unipi.stpin.outputs;

import gr.ds.unipi.stpin.parsers.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.*;

public class RedisClusterOutput implements RedisOutput {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterOutput.class);
    private final int batchSize;
    private int count = 0;
    private final String database;

    private final JedisCluster jedisCluster;

    private final Map<String, Jedis> jedises = new HashMap<>();
    private final Map<String, Pipeline> pipelines = new HashMap<>();

    private final Map <String, JedisPool> nodeMap;
    private final TreeMap <Long, String> slotHostMap;

    public RedisClusterOutput(String host, int port, String database, int batchSize) {

        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort(host,port));

        this.jedisCluster =  RedisClusterConnector.newRedisClusterConnector(nodes).getJedisCluster();
        this.batchSize = batchSize;
        this.database = database;


        nodeMap = jedisCluster.getClusterNodes();

        String anyHost = nodeMap.keySet ().iterator().next();

        // getSlotHostMap method has below

        slotHostMap = getSlotHostMap(anyHost);

    }

    private Pipeline getPipelineBasedOnKey(String key){

        // Get slot number

        int slot = JedisClusterCRC16.getSlot (key);

        // Get the corresponding Jedis object

        Map.Entry <Long, String> entry = slotHostMap.lowerEntry (Long.valueOf (slot));

        Jedis jedis = nodeMap.get(entry.getValue()).getResource();

        if(jedises.containsKey(entry.getValue())){
            return pipelines.get(entry.getValue());
        }
        else{
            jedises.put(entry.getValue(), nodeMap.get (entry.getValue ()).getResource());
            pipelines.put(entry.getValue(), jedises.get(entry.getValue()).pipelined());
            return pipelines.get(entry.getValue());
        }
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
                getPipelineBasedOnKey(database+":"+fieldNames.get(i)).zadd(database+":"+fieldNames.get(i),(double)fieldValues.get(i),primaryKey);

            }
            else if(fieldValues.get(i) instanceof String){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                getPipelineBasedOnKey(database+":"+fieldNames.get(i)+":"+fieldValues.get(i)).sadd(database+":"+fieldNames.get(i)+":"+fieldValues.get(i),primaryKey);
            }
            else if(fieldValues.get(i) instanceof Date){
                map.put(fieldNames.get(i),String.valueOf(((Date) fieldValues.get(i)).getTime()));
                getPipelineBasedOnKey(database+":"+fieldNames.get(i)).zadd(database+":"+fieldNames.get(i),(double)((Date) fieldValues.get(i)).getTime(),primaryKey);
            }
            else if(fieldValues.get(i)==null){
                map.put(fieldNames.get(i),"Null");
                getPipelineBasedOnKey(database+":"+fieldNames.get(i)+":"+"Null").sadd(database+":"+fieldNames.get(i)+":"+"Null",primaryKey);
            }
            else{
                try {
                    throw new Exception("");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        getPipelineBasedOnKey(database+":"+"primaryKeys").sadd(database+":"+"primaryKeys",primaryKey);
        getPipelineBasedOnKey(primaryKey).hset(primaryKey, map);

        count++;
        if(count==batchSize){
            logger.debug("Writing batches to Redis cluster...");
            pipelines.forEach((s,p)->p.sync());
            logger.debug("Done!");
            count=0;
        }

    }

    @Override
    public void close() {

        pipelines.forEach((s,p)->p.sync());
        pipelines.forEach((s,p)->p.close());

        jedises.forEach((i,j)->j.close());

        jedisCluster.close();
    }

    private static TreeMap <Long, String> getSlotHostMap (String anyHostAndPortStr) {
        TreeMap<Long, String> tree = new TreeMap <Long, String> ();
        String parts [] = anyHostAndPortStr.split (":");
        HostAndPort anyHostAndPort = new HostAndPort(parts [0], Integer.parseInt (parts [1]));
        try {
            Jedis jedis = new Jedis (anyHostAndPort.getHost (), anyHostAndPort.getPort ());
            List <Object> list = jedis.clusterSlots ();
            for (Object object: list) {
                List <Object> list1 = (List <Object>) object;
                List <Object> master = (List <Object>) list1.get (2);
                String hostAndPort = new String ((byte []) master.get (0)) + ":" + master.get (1);
                tree.put ((Long) list1.get (0), hostAndPort);
                tree.put ((Long) list1.get (1), hostAndPort);
            }
            jedis.close ();
        } catch (Exception e) {

        }
        return tree;
    }
}
