package com.sephora.data.model;

import java.util.Map;

public class DeltaOperation<T> {

    private String action;
    private String timestamp;
    private Map<String, Object> fields;

    public DeltaOperation() {
    }

    public DeltaOperation(String action, String timestamp, Map<String, Object> fields) {
        this.action = action;
        this.timestamp = timestamp;
        this.fields = fields;
    }

    public String getAction() {
        return action;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "DeltaOperation{" +
                "action='" + action + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", fields=" + fields +
                '}';
    }
}