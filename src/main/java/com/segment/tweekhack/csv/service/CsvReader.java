package com.segment.tweekhack.csv.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.CaseFormat;
import com.segment.tweekhack.common.Constants;
import com.segment.tweekhack.csv.discovery.CsvDiscovery;
import com.segment.tweekhack.kafka.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

@Service
@EnableScheduling
@Slf4j
public class CsvReader {
    private final CsvDiscovery discovery;
    private final Producer processor;
    @Value(value = "${enable.delete}")
    private String enableDelete;

    @Autowired
    public CsvReader(CsvDiscovery discovery, Producer processor) {
        this.discovery = discovery;
        this.processor = processor;
    }

    private static MappingIterator<Map<String, Object>> csvIterator(File file) throws IOException {
        log.trace("Creating mapping iterator");
        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.readerFor(Map.class).with(bootstrap).readValues(file);
    }

    private static String writeAsJson(Map<String, Object> data) throws IOException {
        log.trace("Converting the map to json");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(data);
    }

    @Scheduled(initialDelay = 2000, fixedRate = 60 * 1000)
    public void execute() throws IOException {
        log.info("Job started at {}", Instant.now());

        List<Path> payload = discovery.getPayload();
        log.info("Found {} files at the payload location", payload.size());

        if (!payload.isEmpty()) {
            payload.iterator().forEachRemaining(p -> {
                try {
                    String filename = p.toFile().getName();
                    log.info("Processing file: {}", filename);

                    filename = FilenameUtils.removeExtension(filename);
                    String[] tokens = filename.split(Constants.UNDERSCORE, 3);
                    if (tokens.length < 3) {
                        log.warn("Invalid format of the filename {}, skipping it", filename);
                        return;
                    }

                    String collection = tokens[2];
                    String writeKey = tokens[1];

                    long count = 0;
                    MappingIterator<Map<String, Object>> iterator = csvIterator(p.toFile());
                    while (iterator.hasNext()) {
                        Map<String, Object> data = iterator.next();
                        if (!data.containsKey("id")) {
                            log.warn("Invalid record, id field not present {}", data);
                            return;
                        }
                        convertKeysToSnakeCase(data);
                        data.put(Constants.COLLECTION, collection);
                        data.put(Constants.WRITE_KEY, writeKey);
                        String json = writeAsJson(data);

                        log.info("Sending record to Kafka: {}", json);
                        processor.sendMessage(json);
                        count++;
                    }
                    log.debug("Processed {} rows from file {}", count, filename);

                    if (Boolean.getBoolean(enableDelete)) {
                        log.debug("Deleting file at the location {}", p.toAbsolutePath());
                        Files.delete(p);
                    }
                } catch (IOException e) {
                    log.error("Exception in CSV Reader", e);
                }
            });
        } else {
            log.info("No CSV files found at the payload location. Retrying at next schedule.");
        }
    }

    private void convertKeysToSnakeCase(Map<String, Object> data) {
        Iterator<Map.Entry<String, Object>> mapIter = data.entrySet().iterator();
        Map<String, Object> newMap = new HashMap<>();

        while (mapIter.hasNext()) {
            Map.Entry<String, Object> entry = mapIter.next();
            String key = entry.getKey();
            key = key.replaceAll(Constants.HYPHEN, Constants.UNDERSCORE);
            key = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key);
            key = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key);
            key = key.toLowerCase(Locale.ROOT);
            key = key.replaceAll(Constants.MORE_THAN_ONE_UNDERSCORES_REGEX, Constants.UNDERSCORE);
            newMap.put(key, entry.getValue());
            mapIter.remove();
        }
        data.putAll(newMap);
    }
}
