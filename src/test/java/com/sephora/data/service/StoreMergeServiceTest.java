package com.sephora.data.service;

import com.sephora.data.model.DeltaOperation;
import com.sephora.data.model.Store;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StoreMergeServiceTest {

    @Test
    void shouldApplyStoreDelta() {
        List<Store> full = List.of(
                new Store(1, "A", "addr1", "FR", "IDF", 100, "2020-01-01", "OUVERT", 10),
                new Store(2, "B", "addr2", "US", "CA", 120, "2021-01-01", "OUVERT", 8)
        );

        List<DeltaOperation<?>> delta = List.of(
                new DeltaOperation<>("INSERT", "2025-01-15T10:30:00Z", Map.of(
                        "id", 3,
                        "nom", "C",
                        "adresse", "addr3",
                        "pays", "FR",
                        "region", "PACA",
                        "surface", 180,
                        "date_ouverture", "2025-01-10",
                        "statut", "OUVERT",
                        "nb_employes", 22
                )),
                new DeltaOperation<>("UPDATE", "2025-01-15T11:00:00Z", Map.of(
                        "id", 2,
                        "surface", 200
                )),
                new DeltaOperation<>("DELETE", "2025-01-16T16:00:00Z", Map.of(
                        "id", 1
                ))
        );

        StoreMergeService service = new StoreMergeService();
        List<Store> result = service.applyDelta(full, delta);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(s -> s.getId() == 3));
        assertTrue(result.stream().anyMatch(s -> s.getId() == 2 && s.getSurface() == 200));
        assertFalse(result.stream().anyMatch(s -> s.getId() == 1));
    }
}