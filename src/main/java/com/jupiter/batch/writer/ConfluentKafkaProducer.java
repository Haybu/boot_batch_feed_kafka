package com.jupiter.batch.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hmohamed on 9/9/15.
 */
public class ConfluentKafkaProducer {

    private static final Log log = LogFactory.getLog(ConfluentKafkaProducer.class);

    @Value("${kafka.servers}")
    private String servers;

    @Value("${kafka.acks}")
    private String acks;

    @Value("${kafka.retries}")
    private String retries;

    @Value("${kafka.keyser}")
    private String keyser;

    @Value("${kafka.valueser}")
    private String valueser;

    @Value("${kafka.registry}")
    private String registry;

    private Resource resource;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.key}")
    private String kafkaKey;

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource _resource) {
        this.resource = _resource;
    }

    public void writeToKafka(Map<String, Object> map) {
        log.info("Writing record with map: " + map);
        GenericRecord record = new GenericData.Record(getSchema());

        Object value = null;
        Object kafka_Key_Value = null;
        for(String key: map.keySet()) {
             value = map.get(key);
            log.info("avroing key("+key+") and value ("+value+")");
             record.put(key, value);
             if (key.equalsIgnoreCase(kafkaKey)) {
                 kafka_Key_Value = value;
             }
        }

        // writing to kafka
        ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>(topic, kafkaKey, record);
        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaInstanceProperites());
        producer.send(data);
    }

    public Properties kafkaInstanceProperites(){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("key.serializer", keyser);
        props.put("value.serializer", valueser);
        props.put("schema.registry.url", registry);
        return props;
    }

    public Schema getSchema() {
        String schemaString = null;
        Schema schema = null;

        try {
            File file = new File(resource.getURI());
            FileInputStream fis = new FileInputStream(file);
            StringBuilder builder = new StringBuilder();
            int ch;
            while((ch = fis.read()) != -1){
                builder.append((char)ch);
            }
            schemaString = builder.toString();

        } catch (Exception ex) {
            log.error(ex.getMessage());
        }

        if(schemaString != null) {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(schemaString);
        }

        return schema;
    }

    public void logMap(Map<String, Object> map) {
        if (map == null) {
            log.info("no map passed, map is null");
        }
        for (String key: map.keySet()) {
            log.info("key: " + key + " , value:" + map.get(key)) ;
        }
    }
}
