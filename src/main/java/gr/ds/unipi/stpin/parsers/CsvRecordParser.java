package gr.ds.unipi.stpin.parsers;

import gr.ds.unipi.stpin.datasources.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class CsvRecordParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(CsvRecordParser.class);

    private final int longitudeFieldId;// = AppConfig.config.getInt(inputLongitudeFieldIdSetting);
    private final int latitudeFieldId;// = AppConfig.config.getInt(inputLatitudeFieldIdSetting);
    private final int vehicleFieldId;// = AppConfig.config.getInt(inputVehicleFieldIdSetting);
    private final int dateFieldId;// = AppConfig.config.getInt(inputDateFieldIdSetting);

    private final String separator;
    private String[] headers;
    private String[] types;

    public CsvRecordParser(Datasource source, String separator, String headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source, dateFormat);
        this.separator = separator;
        this.headers = headers.split(separator);
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(Datasource source, String separator, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source, dateFormat);
        this.separator = separator;
        this.headers = null;
        this.vehicleFieldId = -1;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(Datasource source, String separator, String headers) {
        this(source, separator, headers, -1, -1, -1, -1, null);
    }

    public CsvRecordParser(Datasource source, String separator) {
        this(source, separator, -1, -1, -1, null);
    }

    public CsvRecordParser(Datasource source, String separator, int longitudeFieldId, int latitudeFieldId) {
        super(source, null);
        this.separator = separator;
        this.headers = null;
        this.vehicleFieldId = -1;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = -1;

    }

    @Override
    public Record nextRecord() throws ParseException {
        String[] fieldValues = lineWithMeta[0].split(this.separator, -1);
        if ((headers != null) && (fieldValues.length != headers.length)) {
            logger.error("Line has {} fields but {} fields are expected!\nLine: {}", fieldValues.length, headers.length, lineWithMeta[0]);
            throw new ParseException("Wrong input!", 0);
        }

        if(types != null){
            if(types.length != fieldValues.length){
                logger.error("The number of field values do not match with the number of types");
                throw new ParseException("Wrong input!",0);
            }
            fieldValues[dateFieldId-1] = fieldValues[dateFieldId-1];
            Object[] fieldValuesWithType = new Object[fieldValues.length];
            for (int i = 0; i < fieldValues.length; i++) {

                if(types[i].equals("s")){
                    fieldValuesWithType[i] = fieldValues[i];
                }
                else if(types[i].equals("n")){
                    fieldValuesWithType[i] = Double.parseDouble(fieldValues[i]);
                }
                else{
                    logger.error("A value is neither string nor number");
                    throw new ParseException("Wrong input!",0);
                }
            }
            return new Record(fieldValuesWithType, lineWithMeta[1], headers);
        }
        return new Record(fieldValues, lineWithMeta[1], headers);
    }

    @Override
    public String getLatitude(Record record) {
        return String.valueOf(record.getFieldValues().get(latitudeFieldId - 1));
    }

    @Override
    public String getLongitude(Record record) {
        return  String.valueOf(record.getFieldValues().get(longitudeFieldId - 1));
    }

    @Override
    public String getDate(Record record) {
        return String.valueOf(record.getFieldValues().get(dateFieldId - 1));
    }

    public int getDateIndex() {
        return (dateFieldId - 1);
    }

    @Override
    public String getVehicle(Record record) {
        return (String) record.getFieldValues().get(vehicleFieldId - 1);
    }

    @Override
    public String toDefaultOutputFormat(Record record) {
        return toCsv(record, ";");
    }

    @Override
    public RecordParser cloneRecordParser(Datasource source) {
        return new CsvRecordParser(source, separator, headers, vehicleFieldId, longitudeFieldId, latitudeFieldId, dateFieldId, dateFormat);
    }

    @Override
    public String getVehicleFieldName(Record record) {
        return record.getFieldNames().get(vehicleFieldId - 1);
    }

    @Override
    public String getLatitudeFieldName(Record record) {
        return record.getFieldNames().get(latitudeFieldId - 1);
    }

    @Override
    public String getLongitudeFieldName(Record record) {
        return record.getFieldNames().get(longitudeFieldId - 1);
    }

    @Override
    public String getDateFieldName(Record record) {
        return record.getFieldNames().get(dateFieldId - 1);
    }

    private CsvRecordParser(Datasource source, String separator, String[] headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat){
        super(source, dateFormat);
        this.separator = separator;
        this.headers = headers;
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(Datasource source, String separator, String headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat, String types) {
        super(source, dateFormat);
        this.separator = separator;
        this.headers = headers.split(separator);
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
        this.types= types.split(separator);
    }

    public CsvRecordParser(CsvRecordParser csvRecordParser){
        super(csvRecordParser.source, csvRecordParser.dateFormat);
        separator = csvRecordParser.separator;
        headers = csvRecordParser.headers;
        vehicleFieldId = csvRecordParser.vehicleFieldId;
        longitudeFieldId = csvRecordParser.longitudeFieldId;
        latitudeFieldId = csvRecordParser.latitudeFieldId;
        dateFieldId = csvRecordParser.dateFieldId;
    }
}
