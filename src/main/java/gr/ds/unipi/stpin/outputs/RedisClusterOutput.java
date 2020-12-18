package gr.ds.unipi.stpin.outputs;

import gr.ds.unipi.stpin.parsers.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class RedisClusterOutput implements RedisOutput {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterOutput.class);
    private final int batchSize;
    private int count = 0;
    private final String database;

    private final JedisCluster jedisCluster;

    //private final Map<String, Jedis> jedises = new HashMap<>();
    //private final Map<String, Pipeline> pipelines = new HashMap<>();

    private final Map<String, JedisPool> nodeMap;

    private final List<Map.Entry<String, Pipeline>> pipelinesOfNodes;
    private int indexOfPipeline = 0;

    public RedisClusterOutput(String host, int port, String database, int batchSize) {

        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort(host,port));

        this.jedisCluster =  RedisClusterConnector.newRedisClusterConnector(nodes).getJedisCluster();
        this.batchSize = batchSize;
        this.database = database;

        //nodeMap = jedisCluster.getClusterNodes();

        pipelinesOfNodes = new ArrayList<>();

        nodeMap = jedisCluster.getClusterNodes();

        String anyHost = nodeMap.keySet().iterator().next();

        // getSlotHostMap method has below

        getSlotHostMap(anyHost).forEach(entry->
                pipelinesOfNodes.add(new AbstractMap.SimpleImmutableEntry<>("{"+crc16Slot.get(entry.getKey().intValue()+1)+"}",nodeMap.get(entry.getValue()).getResource().pipelined()))
        );
        pipelinesOfNodes.forEach(e-> System.out.println(e.getKey()));
    }

//    private Pipeline getPipelineBasedOnKey(String key){
//
//        // Get slot number
//
//        int slot = JedisClusterCRC16.getSlot(key);
//
//        // Get the corresponding Jedis object
//
//        Map.Entry <Long, String> entry = slotHostMap.lowerEntry(Long.valueOf(slot));
//
//        if(entry == null){//if slot is 0
//            entry = slotHostMap.lowerEntry(Long.valueOf(slot+1));
//        }
//
//        if(jedises.containsKey(entry.getValue())){
//            return pipelines.get(entry.getValue());
//        }
//        else{
//            jedises.put(entry.getValue(), nodeMap.get(entry.getValue()).getResource());
//            pipelines.put(entry.getValue(), jedises.get(entry.getValue()).pipelined());
//            return pipelines.get(entry.getValue());
//        }
//    }

    @Override
    public void out(Record record, String lineMetaData) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        String primaryKey = RandomGenerator.randomCharacterNumericString()+pipelinesOfNodes.get(indexOfPipeline).getKey();

        //String primaryKey = lineMetaData+pipelinesOfNodes.get(indexOfPipeline).getKey();

        Map<String,String> map = new HashMap<>();

        //for baseline
//        double longitude =0;
//        double latitude =0;

        for (int i = 0; i < fieldNames.size(); i++) {

            if(!(fieldNames.get(i).equals("longitude") || fieldNames.get(i).equals("latitude") || fieldNames.get(i).equals("localDate") || fieldNames.get(i).equals("vehicle"))){
                continue;
            }

            if(fieldValues.get(i) instanceof Number){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                //pipelinesOfNodes.get(indexOfPipeline).getValue().zadd(database+":"+fieldNames.get(i)+pipelinesOfNodes.get(indexOfPipeline).getKey(),(double)fieldValues.get(i),primaryKey);

                //for baseline
//                if(fieldNames.get(i).equals("longitude")){
//                    longitude = (double) fieldValues.get(i);
//                }
//                else{
//                    latitude = (double) fieldValues.get(i);
//                }
            }
            else if(fieldValues.get(i) instanceof String){
                map.put(fieldNames.get(i),String.valueOf(fieldValues.get(i)));
                //pipelinesOfNodes.get(indexOfPipeline).getValue().sadd(database+":"+fieldNames.get(i)+":"+fieldValues.get(i)+pipelinesOfNodes.get(indexOfPipeline).getKey(),primaryKey);
            }
            else if(fieldValues.get(i) instanceof Date){
                map.put(fieldNames.get(i),String.valueOf(((Date) fieldValues.get(i)).getTime()));
                //pipelinesOfNodes.get(indexOfPipeline).getValue().zadd(database+":"+fieldNames.get(i)+pipelinesOfNodes.get(indexOfPipeline).getKey(),(double)((Date) fieldValues.get(i)).getTime(),primaryKey);

                //for baseline
//                pipelinesOfNodes.get(indexOfPipeline).getValue().zadd(database+":"+"localDate"+pipelinesOfNodes.get(indexOfPipeline).getKey(), ((Date) fieldValues.get(i)).getTime(), primaryKey);
            }
            else if(fieldValues.get(i)==null){
                map.put(fieldNames.get(i),"Null");
                //pipelinesOfNodes.get(indexOfPipeline).getValue().sadd(database+":"+fieldNames.get(i)+":"+"Null"+pipelinesOfNodes.get(indexOfPipeline).getKey(),primaryKey);
            }
            else{
                try {
                    throw new Exception("");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //for baseline
        //pipelinesOfNodes.get(indexOfPipeline).getValue().geoadd(database+":"+"location"+pipelinesOfNodes.get(indexOfPipeline).getKey(),longitude,latitude, primaryKey);

        //pipelinesOfNodes.get(indexOfPipeline).getValue().sadd(database+":"+"location"+pipelinesOfNodes.get(indexOfPipeline).getKey(),lineMetaData + "-" + primaryKey);

        pipelinesOfNodes.get(indexOfPipeline).getValue().zadd(database+":"+"location:localDate"+pipelinesOfNodes.get(indexOfPipeline).getKey(),Long.valueOf(lineMetaData), primaryKey);
        pipelinesOfNodes.get(indexOfPipeline).getValue().sadd(database+":"+"primaryKeys"+pipelinesOfNodes.get(indexOfPipeline).getKey(),primaryKey);
        pipelinesOfNodes.get(indexOfPipeline).getValue().hset(primaryKey, map);

        count++;
        if(count==batchSize){
            logger.debug("Writing batches to Redis cluster...");
            //pipelines.forEach((s,p)->p.sync());
            pipelinesOfNodes.forEach(e->e.getValue().sync());
            logger.debug("Done!");
            count=0;
        }

        indexOfPipeline++;
        if(indexOfPipeline ==pipelinesOfNodes.size()){
            indexOfPipeline = 0;
        }

    }

    @Override
    public void close() {

//        pipelines.forEach((s,p)->p.sync());
////        pipelines.forEach((s,p)->p.close());
        pipelinesOfNodes.forEach(e->e.getValue().sync());

        nodeMap.forEach((s,j)->j.close());

        pipelinesOfNodes.forEach(e->e.getValue().close());

        //jedises.forEach((i,j)->j.close());

        jedisCluster.close();
    }

//    private static TreeMap <Long, String> getSlotHostMap (String anyHostAndPortStr) {
//        TreeMap<Long, String> tree = new TreeMap <Long, String> ();
//        String parts [] = anyHostAndPortStr.split (":");
//        HostAndPort anyHostAndPort = new HostAndPort(parts [0], Integer.parseInt (parts [1]));
//        try {
//            Jedis jedis = new Jedis (anyHostAndPort.getHost (), anyHostAndPort.getPort ());
//            List <Object> list = jedis.clusterSlots ();
//            for (Object object: list) {
//                List <Object> list1 = (List <Object>) object;
//                List <Object> master = (List <Object>) list1.get (2);
//                String hostAndPort = new String ((byte []) master.get (0)) + ":" + master.get (1);
//                tree.put ((Long) list1.get (0), hostAndPort);
//                tree.put ((Long) list1.get (1), hostAndPort);
//            }
//            jedis.close ();
//        } catch (Exception e) {
//
//        }
//        return tree;
//    }
    private static List<Map.Entry<Long, String>> getSlotHostMap (String anyHostAndPortStr) {
        List<Map.Entry<Long, String>> entries = new ArrayList<>();
        String parts [] = anyHostAndPortStr.split (":");
        HostAndPort anyHostAndPort = new HostAndPort(parts [0], Integer.parseInt (parts [1]));
        try {
            Jedis jedis = new Jedis (anyHostAndPort.getHost (), anyHostAndPort.getPort ());
            List <Object> list = jedis.clusterSlots ();
            for (Object object: list) {
                List <Object> list1 = (List <Object>) object;
                List <Object> master = (List <Object>) list1.get (2);
                String hostAndPort = new String ((byte []) master.get (0)) + ":" + master.get (1);
                entries.add(new AbstractMap.SimpleImmutableEntry<>((Long) list1.get (0), hostAndPort));
            }
            jedis.close ();
        } catch (Exception e) {

        }
        return entries;
    }

    private static List<String> crc16Slot = new BufferedReader(new InputStreamReader(RedisConnector.class.getResourceAsStream("/codes.txt"))).lines().collect(Collectors.toList());

}
