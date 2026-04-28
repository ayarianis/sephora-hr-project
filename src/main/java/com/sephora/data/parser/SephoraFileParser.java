package com.sephora.data.parser;

import java.util.List;

public interface SephoraFileParser<T> {
    List<T> parse(String filePath) throws Exception;
}