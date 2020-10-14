package gr.ds.unipi.stpin.outputs;

import gr.ds.unipi.stpin.parsers.Record;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;

public class HBaseOutput implements Output<Record> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseOutput.class);

    private final Connection connection;
    private final Table table;
    private final int batchSize;
    private int count = 0;
    private List<Put> putList = new ArrayList<>();

    public HBaseOutput(String host, int port, String table, int batchSize) throws IOException {
        this.connection =  HBaseConnector.newHBaseConnector(host).getConnection();
        this.table = connection.getTable(TableName.valueOf(table));
        this.batchSize = batchSize;
    }

    @Override
    public void out(Record record, String lineMetaData) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        String primaryKey = lineMetaData;

        for (int i = 0; i < fieldNames.size(); i++) {

            Put put = new Put(Bytes.toBytes(primaryKey));
            String[] cols = fieldNames.get(i).split(":");

            if(fieldValues.get(i) instanceof Number){
                put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]),Bytes.toBytes( (double) fieldValues.get(i)));
            }
            else if(fieldValues.get(i) instanceof String){
                put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]),Bytes.toBytes( (String) fieldValues.get(i)));
            }
            else if(fieldValues.get(i) instanceof Date){
                put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]),Bytes.toBytes( (long) fieldValues.get(i)));
            }
            else{
                try {
                    throw new Exception("");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            putList.add(put);
        }

        count++;
        if(count==batchSize){
            logger.debug("Writing batch to HBase...");
            try {
                table.put(putList);
            } catch (IOException e) {
                e.printStackTrace();
            }
            putList.clear();
            logger.debug("Done!");
            count=0;
        }

    }

    @Override
    public void close() {
        try {
            if(putList.size()>0){
                table.put(putList);
                putList.clear();

            }
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
