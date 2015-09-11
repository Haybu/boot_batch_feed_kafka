package com.jupiter.batch.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by hmohamed on 9/10/15.
 */
public class JsonFileItemReader<T>
        extends AbstractItemCountingItemStreamItemReader<T> implements
        ResourceAwareItemReaderItemStream<T>, InitializingBean {

    private static final Log logger = LogFactory.getLog(JsonFileItemReader.class);

    // default encoding for input files
    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();


    private Resource resource;

    private int partCount = 0;

    private String[] comments = new String[] { "#" };

    private boolean noInput = false;

    private String encoding = DEFAULT_CHARSET;

    private int partsToSkip = 0;

    private boolean strict = true;

    private JsonFileParser jsonFileParser;

    public JsonFileItemReader() {
        setName(ClassUtils.getShortName(FlatFileItemReader.class));
    }


    public void setJsonFileParser(JsonFileParser parser) {
        jsonFileParser = parser;
    }

    /**
     * In strict mode the reader will throw an exception on
     * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input resource does not exist.
     * @param strict <code>true</code> by default
     */
    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    /**
     * Public setter for the number of lines to skip at the start of a file. Can be used if the file contains a header
     * without useful (column name) information, and without a comment delimiter at the beginning of the lines.
     *
     * @param linesToSkip the number of lines to skip
     */
    public void setLinesToSkip(int linesToSkip) {
        this.partsToSkip = linesToSkip;
    }


    /**
     * Setter for the encoding for this input source. Default value is {@link #DEFAULT_CHARSET}.
     *
     * @param encoding a properties object which possibly contains the encoding for this input file;
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Setter for comment prefixes. Can be used to ignore header lines as well by using e.g. the first couple of column
     * names as a prefix.
     *
     * @param comments an array of comment line prefixes.
     */
    public void setComments(String[] comments) {
        this.comments = new String[comments.length];
        System.arraycopy(comments, 0, this.comments, 0, comments.length);
    }

    /**
     * Public setter for the input resource.
     */
    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    protected T doRead() throws Exception {
        if (noInput) {
            return null;
        }

        return (T) readItem();
    }

    /**
     * @return next line (skip comments).getCurrentResource
     */
    private Map<String, Object> readItem() {

        if (jsonFileParser == null) {
            throw new ReaderNotOpenException("No json parser. Reader must be open before it can be read.");
        }

        Map<String, Object> map = null;

        try {
            if (jsonFileParser.hasNext()) {
                map = this.jsonFileParser.next();
            }
            if (map == null) {
                return null;
            }
            partCount++;
        }
        catch (Exception e) {
            // Prevent IOException from recurring indefinitely
            // if client keeps catching and re-calling
            noInput = true;
            throw new NonTransientFlatFileException("Unable to read from resource: [" + resource + "]", e, map.toString(),
                    partCount);
        }
        return map;
    }


    @Override
    protected void doClose() throws Exception {
        partCount = 0;
        if (jsonFileParser != null) {
            jsonFileParser.close();
        }
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(resource, "Input resource must be set");

        noInput = true;
        if (!resource.exists()) {
            if (strict) {
                throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): " + resource);
            }
            logger.warn("Input resource does not exist " + resource.getDescription());
            return;
        }

        if (!resource.isReadable()) {
            if (strict) {
                throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): "
                        + resource);
            }
            logger.warn("Input resource is not readable " + resource.getDescription());
            return;
        }

        jsonFileParser = new JsonFileParser(resource.getInputStream());
        Map<String, Object> map = null;
        if (partsToSkip > 0) {
            int i = 0;
            while(jsonFileParser.hasNext() && i < partsToSkip) {
                map = jsonFileParser.next();
                i++;
            }
        }
        noInput = false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //Assert.notNull(lineMapper, "LineMapper is required");
    }

    @Override
    protected void jumpToItem(int itemIndex) throws Exception {
        Map<String, Object> map = null;
        int i = 0;
        while(jsonFileParser.hasNext() && i < itemIndex) {
            map = jsonFileParser.next();
            i++;
        }
    }

}
