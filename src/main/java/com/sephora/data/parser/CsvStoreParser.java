package com.sephora.data.parser;

import com.sephora.data.model.Store;
import com.sephora.data.service.StoreService;

import java.util.List;

public class CsvStoreParser implements SephoraFileParser<Store> {

    @Override
    public List<Store> parse(String filePath) throws Exception {
        StoreService service = new StoreService();
        return service.loadStoresFromCsv(filePath);
    }
}