package gr.ds.unipi.stpin.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Record {

    private static final Logger logger = LoggerFactory.getLogger(Record.class);

    private List<Object> fieldValues;
    private List<String> fieldNames;
    private String metadata;

    private Record(Object[] fieldValues, String metadata) {
        this.fieldValues = new ArrayList<>(Arrays.asList(fieldValues));
        this.metadata = metadata;
    }

    public Record(Object[] fieldValues, String metadata, String[] fieldNames) {
        this(fieldValues, metadata);
        if (fieldNames != null) {
            this.fieldNames = new ArrayList<>(Arrays.asList(fieldNames));
        }
    }

    public String getMetadata() {
        return metadata;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Object> getFieldValues() {
        return fieldValues;
    }

    public void addFieldValues(List<Object> newFieldValues) {
        this.fieldValues.addAll(newFieldValues);
    }

    public void addFieldNames(List<String> newFieldNames) {
        this.fieldNames.addAll(newFieldNames);
    }

    public void addFieldValue(Object newFieldValue) {
        this.fieldValues.add(newFieldValue);
    }

    public void addFieldName(String newFieldName) {
        this.fieldNames.add(newFieldName);
    }

    public void deleteLastFieldName() {
        this.fieldNames.remove(fieldNames.size() - 1);
    }

    public void deleteLastFieldValue() {
        this.fieldValues.remove(fieldValues.size() - 1);
    }
}
