package com.nebutown.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class Json {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    /**
     * Encode an object into a JSON string. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    public static String encodeAsString(Object obj) throws JsonProcessingException {

        return OBJECT_MAPPER.writeValueAsString(obj);
    }

    /**
     * Encode an object into a JSON value in bytes. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    public static byte[] encodeAsBytes(Object obj) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsBytes(obj);
    }

    public static <T> T decodeObject(byte[] bytes, Class<T> glass) throws IOException {

        return OBJECT_MAPPER.readValue(bytes, glass);
    }

    public static String toJson(Object value) {
        try{

            return encodeAsString(value);
        } catch (JsonProcessingException e) {
//            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }


    public static <T> T fromJson(Class<T> type, String data) {
        try {
            return OBJECT_MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException("Conversion from JSON failed", e);
        }
    }

    public static <T> T fromJson(TypeReference<T> type, String data) {
        try {
            return OBJECT_MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException("Conversion from JSON failed", e);
        }
    }

    public static <T> T fromJson(TypeReference<T> type, byte[] data) {
        try {
            return OBJECT_MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException("Conversion from JSON failed", e);
        }
    }

    public static Map<String, String> mapFromJson(String o) {
        try {
            return OBJECT_MAPPER.readValue(o, new TypeReference<Map<String, String>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Conversion from JSON failed", e);
        }
    }
}
