package com.sephora.data.service;

import com.sephora.data.model.DeltaOperation;
import com.sephora.data.model.Employee;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EmployeeMergeService {

    public List<Employee> applyDelta(List<Employee> fullData, List<DeltaOperation<?>> delta) {
        Map<Integer, Employee> employeeMap = fullData.stream()
                .collect(Collectors.toMap(Employee::getId, e -> e));

        for (DeltaOperation<?> op : delta) {
            Map<String, Object> fields = op.getFields();
            int id = toInt(fields.get("id"));

            switch (op.getAction()) {
                case "INSERT":
                    Employee inserted = buildEmployeeFromFields(fields);
                    employeeMap.put(id, inserted);
                    break;

                case "UPDATE":
                    Employee existing = employeeMap.get(id);
                    if (existing != null) {
                        Employee updated = updateEmployee(existing, fields);
                        employeeMap.put(id, updated);
                    }
                    break;

                case "DELETE":
                    employeeMap.remove(id);
                    break;

                default:
                    throw new IllegalArgumentException("Action inconnue : " + op.getAction());
            }
        }

        return new ArrayList<>(employeeMap.values());
    }

    private Employee buildEmployeeFromFields(Map<String, Object> fields) {
        return new Employee(
                toInt(fields.get("id")),
                toStr(fields.get("nom")),
                toStr(fields.get("prenom")),
                toInt(fields.get("store_id")),
                toStr(fields.get("poste")),
                toStr(fields.get("departement")),
                toStr(fields.get("date_embauche")),
                toStr(fields.get("type_contrat")),
                toDouble(fields.get("salaire_brut"))
        );
    }

    private Employee updateEmployee(Employee existing, Map<String, Object> fields) {
        int id = fields.containsKey("id") ? toInt(fields.get("id")) : existing.getId();
        String nom = fields.containsKey("nom") ? toStr(fields.get("nom")) : existing.getNom();
        String prenom = fields.containsKey("prenom") ? toStr(fields.get("prenom")) : existing.getPrenom();
        int storeId = fields.containsKey("store_id") ? toInt(fields.get("store_id")) : existing.getStoreId();
        String poste = fields.containsKey("poste") ? toStr(fields.get("poste")) : existing.getPoste();
        String departement = fields.containsKey("departement") ? toStr(fields.get("departement")) : existing.getDepartement();
        String dateEmbauche = fields.containsKey("date_embauche") ? toStr(fields.get("date_embauche")) : existing.getDateEmbauche();
        String typeContrat = fields.containsKey("type_contrat") ? toStr(fields.get("type_contrat")) : existing.getTypeContrat();
        double salaireBrut = fields.containsKey("salaire_brut") ? toDouble(fields.get("salaire_brut")) : existing.getSalaireBrut();

        return new Employee(id, nom, prenom, storeId, poste, departement, dateEmbauche, typeContrat, salaireBrut);
    }

    private int toInt(Object value) {
        if (value == null) return 0;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        return Integer.parseInt(String.valueOf(value));
    }

    private double toDouble(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Double) return (Double) value;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(String.valueOf(value));
    }

    private String toStr(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}