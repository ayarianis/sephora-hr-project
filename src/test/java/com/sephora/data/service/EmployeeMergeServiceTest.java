package com.sephora.data.service;

import com.sephora.data.model.DeltaOperation;
import com.sephora.data.model.Employee;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EmployeeMergeServiceTest {

    @Test
    void shouldApplyEmployeeDelta() {
        List<Employee> full = List.of(
                new Employee(1, "Roux", "Maxime", 1, "Stockiste", "Logistique", "2024-12-21", "CDI", 23365),
                new Employee(2, "Robert", "Chloe", 1, "Assistant(e) RH", "RH", "2018-07-09", "Stage", 10992)
        );

        List<DeltaOperation<?>> delta = List.of(
                new DeltaOperation<>("INSERT", "2025-02-01T09:00:00Z", Map.of(
                        "id", 810,
                        "nom", "Dupont",
                        "prenom", "Sophie",
                        "store_id", 1,
                        "poste", "Conseiller(e) Beaute",
                        "departement", "Vente",
                        "date_embauche", "2025-01-28",
                        "type_contrat", "CDI",
                        "salaire_brut", 25500
                )),
                new DeltaOperation<>("UPDATE", "2025-02-01T10:15:00Z", Map.of(
                        "id", 2,
                        "salaire_brut", 12000
                )),
                new DeltaOperation<>("DELETE", "2025-02-01T11:00:00Z", Map.of(
                        "id", 1
                ))
        );

        EmployeeMergeService service = new EmployeeMergeService();
        List<Employee> result = service.applyDelta(full, delta);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(e -> e.getId() == 810));
        assertTrue(result.stream().anyMatch(e -> e.getId() == 2 && e.getSalaireBrut() == 12000));
        assertFalse(result.stream().anyMatch(e -> e.getId() == 1));
    }
}