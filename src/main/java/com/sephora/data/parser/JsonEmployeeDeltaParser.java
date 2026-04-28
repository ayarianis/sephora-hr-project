package com.sephora.data.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sephora.data.model.DeltaOperation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonEmployeeDeltaParser implements SephoraFileParser<DeltaOperation<?>> {

    @Override
    public List<DeltaOperation<?>> parse(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> rawDelta;

        if (filePath.startsWith("classpath:")) {
            String cp = filePath.substring("classpath:".length());
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cp);
            if (is == null) {
                throw new IOException("Resource not found on classpath: " + cp);
            }
            rawDelta = mapper.readValue(is, new TypeReference<List<Map<String, Object>>>() {});
        } else {
            rawDelta = mapper.readValue(
                    Files.newInputStream(Paths.get(filePath)),
                    new TypeReference<List<Map<String, Object>>>() {}
            );
        }

        return rawDelta.stream()
                .map(item -> {
                    String action = (String) item.get("action");
                    String timestamp = (String) item.get("timestamp");

                    Map<String, Object> fields = new HashMap<>(item);
                    fields.remove("action");
                    fields.remove("timestamp");

                    return new DeltaOperation<>(action, timestamp, fields);
                })
                .collect(Collectors.toList());
    }
}