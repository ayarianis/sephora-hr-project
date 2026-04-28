package com.sephora.data.service;

import com.sephora.data.model.DeltaOperation;
import com.sephora.data.model.Store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StoreMergeService {

    public List<Store> applyDelta(List<Store> fullData, List<DeltaOperation<?>> delta) {
        Map<Integer, Store> storeMap = fullData.stream()
                .collect(Collectors.toMap(Store::getId, s -> s));

        for (DeltaOperation<?> op : delta) {
            Map<String, Object> fields = op.getFields();
            int id = toInt(fields.get("id"));

            switch (op.getAction()) {
                case "INSERT":
                    Store inserted = buildStoreFromFields(fields);
                    storeMap.put(id, inserted);
                    break;

                case "UPDATE":
                    Store existing = storeMap.get(id);
                    if (existing != null) {
                        Store updated = updateStore(existing, fields);
                        storeMap.put(id, updated);
                    }
                    break;

                case "DELETE":
                    storeMap.remove(id);
                    break;

                default:
                    throw new IllegalArgumentException("Action inconnue : " + op.getAction());
            }
        }

        return new ArrayList<>(storeMap.values());
    }

    private Store buildStoreFromFields(Map<String, Object> fields) {
        return new Store(
                toInt(fields.get("id")),
                toStr(fields.get("nom")),
                toStr(fields.get("adresse")),
                toStr(fields.get("pays")),
                toStr(fields.get("region")),
                toInt(fields.get("surface")),
                readDateOuverture(fields),
                toStr(fields.get("statut")),
                readNbEmployes(fields)
        );
    }

    private Store updateStore(Store existing, Map<String, Object> fields) {
        int id = fields.containsKey("id") ? toInt(fields.get("id")) : existing.getId();
        String nom = fields.containsKey("nom") ? toStr(fields.get("nom")) : existing.getNom();
        String adresse = fields.containsKey("adresse") ? toStr(fields.get("adresse")) : existing.getAdresse();
        String pays = fields.containsKey("pays") ? toStr(fields.get("pays")) : existing.getPays();
        String region = fields.containsKey("region") ? toStr(fields.get("region")) : existing.getRegion();
        int surface = fields.containsKey("surface") ? toInt(fields.get("surface")) : existing.getSurface();
        String dateOuverture = fields.containsKey("dateOuverture") || fields.containsKey("date_ouverture")
                ? readDateOuverture(fields)
                : existing.getDateOuverture();
        String statut = fields.containsKey("statut") ? toStr(fields.get("statut")) : existing.getStatus();
        int nbEmployes = fields.containsKey("nbEmployes") || fields.containsKey("nb_employes")
                ? readNbEmployes(fields)
                : existing.getNbEmployes();

        return new Store(id, nom, adresse, pays, region, surface, dateOuverture, statut, nbEmployes);
    }

    private String readDateOuverture(Map<String, Object> fields) {
        if (fields.containsKey("dateOuverture")) {
            return toStr(fields.get("dateOuverture"));
        }
        return toStr(fields.get("date_ouverture"));
    }

    private int readNbEmployes(Map<String, Object> fields) {
        if (fields.containsKey("nbEmployes")) {
            return toInt(fields.get("nbEmployes"));
        }
        return toInt(fields.get("nb_employes"));
    }

    private int toInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private String toStr(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}