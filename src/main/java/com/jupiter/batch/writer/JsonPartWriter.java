package com.jupiter.batch.writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Created by hmohamed on 9/10/15.
 */
public class JsonPartWriter implements ItemWriter {

    private static final Log log = LogFactory.getLog(JsonPartWriter.class);

    ConfluentKafkaProducer producer;

    public ConfluentKafkaProducer getProducer() {
        return producer;
    }

    public void setProducer(ConfluentKafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public void write(List items) throws Exception {
        log.info("part: ");
        items.stream().forEach(map -> producer.writeToKafka((Map<String, Object>)map));
    }
}
