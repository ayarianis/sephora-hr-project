package com.sephora.data.parser;

public class FileParserFactory {

    public static SephoraFileParser<?> getParser(String filePath) {
        String lower = filePath.toLowerCase();

        if (lower.endsWith(".csv") && lower.contains("store")) {
            return new CsvStoreParser();
        }
        if (lower.endsWith(".csv") && lower.contains("employee")) {
            return new CsvEmployeeParser();
        }
        if (lower.endsWith(".json") && lower.contains("store")) {
            return new JsonStoreDeltaParser();
        }
        if (lower.endsWith(".json") && lower.contains("employee")) {
            return new JsonEmployeeDeltaParser();
        }

        throw new IllegalArgumentException("Format non supporté : " + filePath);
    }
}