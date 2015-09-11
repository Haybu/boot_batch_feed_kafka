package com.jupiter.batch.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by hmohamed on 9/10/15.
 */
public class JsonPartProcessor implements ItemProcessor<Object,Object> {

    private static final Log log = LogFactory.getLog(JsonPartProcessor.class);

    @Override
    public Object process(Object item) throws Exception {
        log.info("do nothing in processor..") ;
        return item;
    }
}
