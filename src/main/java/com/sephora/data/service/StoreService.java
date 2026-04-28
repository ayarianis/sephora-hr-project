package com.sephora.data.service;

import com.sephora.data.model.Store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class StoreService {

    public List<Store> loadStoresFromCsv(String filePath) throws IOException {
        try (BufferedReader br = openReader(filePath)) {
            List<Store> stores = new ArrayList<>();

            String rawLine = br.readLine();
            if (rawLine == null) {
                return stores;
            }

            while ((rawLine = br.readLine()) != null) {
                String[] cols = rawLine.split(",", -1);

                for (int i = 0; i < cols.length; i++) {
                    cols[i] = cols[i].trim().replace("\"", "");
                }

                if (cols.length < 9) {
                    continue;
                }

                try {
                    int id = Integer.parseInt(cols[0]);
                    String nom = cols[1];
                    String adresse = cols[2];
                    String pays = cols[3];
                    String region = cols[4];
                    int surface = Integer.parseInt(cols[5]);
                    String dateOuverture = cols[6];
                    String statut = cols[7];
                    int nbEmployes = Integer.parseInt(cols[8]);

                    stores.add(new Store(id, nom, adresse, pays, region, surface, dateOuverture, statut, nbEmployes));
                } catch (NumberFormatException ex) {
                    // ignore ligne invalide
                }
            }

            return stores;
        }
    }

    private BufferedReader openReader(String filePath) throws IOException {
        if (filePath.startsWith("classpath:")) {
            String cp = filePath.substring("classpath:".length());
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cp);
            if (is == null) {
                throw new IOException("Resource not found on classpath: " + cp);
            }
            return new BufferedReader(new InputStreamReader(is));
        } else {
            return Files.newBufferedReader(Paths.get(filePath));
        }
    }

    public Map<String, Long> countStoresByCountry(List<Store> stores) {
        return stores.stream()
                .filter(store -> store.getPays() != null && !store.getPays().isEmpty())
                .collect(Collectors.groupingBy(Store::getPays, Collectors.counting()));
    }

    public Optional<Store> findOldestStore(List<Store> stores) {
        return stores.stream()
                .filter(store -> store.getDateOuverture() != null && !store.getDateOuverture().isEmpty())
                .min(Comparator.comparing(Store::getDateOuverture));
    }

    public List<Map.Entry<String, Long>> top5RegionsByStoreCount(List<Store> stores) {
        return stores.stream()
                .filter(store -> store.getRegion() != null && !store.getRegion().isEmpty())
                .collect(Collectors.groupingBy(Store::getRegion, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .limit(5)
                .collect(Collectors.toList());
    }

    public Map<String, Integer> totalSurfaceByCountry(List<Store> stores) {
        return stores.stream()
                .filter(store -> store.getPays() != null && !store.getPays().isEmpty())
                .collect(Collectors.groupingBy(Store::getPays, Collectors.summingInt(Store::getSurface)));
    }

    public List<Store> findStoresInRenovationSortedBySurface(List<Store> stores) {
        return stores.stream()
                .filter(store -> "RENOVATION".equalsIgnoreCase(store.getStatus()))
                .sorted((a, b) -> Integer.compare(b.getSurface(), a.getSurface()))
                .collect(Collectors.toList());
    }

    public List<Store> filterStoresAfter(List<Store> stores, String year) {
        return stores.stream()
                .filter(store -> store.getDateOuverture() != null)
                .filter(store -> store.getDateOuverture().compareTo(year + "-12-31") > 0)
                .collect(Collectors.toList());
    }
}