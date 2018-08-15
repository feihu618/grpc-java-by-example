package com.nebutown.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
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
}
