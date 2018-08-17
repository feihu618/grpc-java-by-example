package com.nebutown.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.InputStream;

public class ConfigTest {

    @Test
    public void getProperty() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            final InputStream inputStream = ConfigTest.class.getClassLoader().getResourceAsStream("config.yaml");
            Config config = mapper.readValue(inputStream, Config.class);
            System.out.println(config.getClusterName() +"--"+ config.getNodeId());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
