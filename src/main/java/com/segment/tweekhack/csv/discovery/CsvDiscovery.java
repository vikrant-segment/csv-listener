package com.segment.tweekhack.csv.discovery;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Slf4j
public class CsvDiscovery {
    @Value(value = "${payload.location}")
    private String payloadDir;

    public List<Path> getPayload() throws IOException {
        Path payloadPath = Paths.get(payloadDir);
        log.info("Getting payload from {}", payloadPath.toAbsolutePath());

        if (Files.exists(payloadPath) && Files.isDirectory(payloadPath)) {
            try (Stream<Path> walk = Files.walk(payloadPath)) {
                List<Path> files = walk.filter(Files::isRegularFile)
                        .collect(Collectors.toList());
                log.debug("Found files at the payload location {}", files);
                return files;
            }
        }

        log.info("No CSV files at payload location");
        return null;
    }
}
