package com.example.grpc.server;

import com.datastax.driver.mapping.annotations.Table;
import com.example.grpc.TRecord;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Table(name = "record1")
public class Record {
    private String id;
    private String object;
    private Long version;

    public static Record of(TRecord record) {

        Record record1 = new Record();

        record1.setId(record.getKey());
        record1.setObject(record.getValue());
        record1.setVersion(record.getVersion());

        return record1;
    }

    public Record() {

    }
    @JsonCreator
    public Record(@JsonProperty("id")String id, @JsonProperty("object")String object, @JsonProperty("version")Long version) {
        this.id = id;
        this.object = object;
        this.version = version;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id='" + id + '\'' +
                ", object='" + object + '\'' +
                ", version=" + version +
                '}';
    }


}
