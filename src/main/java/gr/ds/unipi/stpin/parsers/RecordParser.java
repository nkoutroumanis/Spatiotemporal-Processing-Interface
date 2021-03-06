package gr.ds.unipi.stpin.parsers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import gr.ds.unipi.stpin.datasources.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(RecordParser.class);

    protected Datasource source;
    protected String[] lineWithMeta;
    protected final String dateFormat;
    private static DateFormat dateFormatPattern;

    public RecordParser(Datasource source, String dateFormat) {
        this.source = source;
        this.dateFormat = dateFormat;
    }

    public abstract Record nextRecord() throws ParseException;

    public boolean hasNextRecord() throws IOException {
        lineWithMeta = new String[]{"", ""};
        while (((lineWithMeta[0].length() == 0) || !isLineValid()) && this.source.hasNextLine()) {
            lineWithMeta = this.source.nextLine();
            lineWithMeta[0] = lineWithMeta[0].trim();
        }
        return lineWithMeta[0].length() != 0;
    }

    protected boolean isLineValid() {
        return true;
    }


    public String toJsonString(Record record) {
        return toConfig(record).root().render(ConfigRenderOptions.concise());
    }

    public Config toConfig(Record record) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        if ((record.getFieldNames() == null) || (record.getFieldNames().size() != record.getFieldValues().size())) {
            logger.error("Field names is wrong!");
            return null;
        }

        Map<String, Object> properties = new HashMap<>();
        for(int i =0; i<fieldNames.size();i++){
            properties.put(fieldNames.get(i), fieldValues.get(i));
        }

        return ConfigFactory.parseMap(properties);

    }

    public String toCsv(Record record, String separator) {

        StringBuilder sb = new StringBuilder();

        sb.append(record.getFieldValues().get(0));
        for (int i = 1; i < record.getFieldValues().size(); i++) {
            sb.append(separator);
            sb.append(record.getFieldValues().get(i));
        }

        return sb.toString();
    }

    public String getLatitude(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getLongitude(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getDate(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getVehicleFieldName(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getLatitudeFieldName(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getLongitudeFieldName(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getDateFieldName(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    public String getVehicle(Record record) {
        throw new UnsupportedOperationException();
    }

    public Datasource getDatasource() {
        return source;
    }

    public abstract String toDefaultOutputFormat(Record record);

    public abstract RecordParser cloneRecordParser(Datasource datasource);

    public Function<Record,Date> getDateFunction(){

        if(getDateFormat().equals("unixTimestampSec")){
            return dateFunctionUnixTimestampSec();
        }
        else if(getDateFormat().equals("unixTimestampMillis")){
            return dateFunctionUnixTimestampMillis();
        }
        else if(getDateFormat().equals("unixTimestampDecimals")){
            return dateFunctionUnixTimestampDecimals();
        }
        else{
            setSimpleDateFormat();
            return dateFunctionDateFormatPattern();
        }

    }

    private Function<Record, Date> dateFunctionUnixTimestampSec(){
         return (record) -> new Date(Long.parseLong(getDate(record)) * 1000l);
    }

    private Function<Record, Date> dateFunctionUnixTimestampMillis(){
        return (record) -> new Date(Long.parseLong(getDate(record)));
    }

    private Function<Record, Date> dateFunctionUnixTimestampDecimals(){
         return (record) -> new Date((long) (Double.parseDouble(getDate(record)) * 1000d));
    }

    private void setSimpleDateFormat(){
        try{
            dateFormatPattern = new SimpleDateFormat(getDateFormat());
        }
        catch (Exception e){
            logger.error("Date Format has not been set correctly");
        }
    }

    private Function<Record, Date> dateFunctionDateFormatPattern(){
         return (record) -> {
             Date d = null;
             try {
                 d = dateFormatPattern.parse(getDate(record));
             } catch (ParseException e) {
                 logger.warn("Temporal information of record can not be parsed {} \nLine {}", e, record.getMetadata());
             }
             return d;
         };
    }
}
