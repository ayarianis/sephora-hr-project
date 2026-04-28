package com.sephora.data.service;

import com.sephora.data.model.Employee;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class EmployeeService {

    public List<Employee> loadEmployeesFromCsv(String filePath) throws IOException {
        try (BufferedReader br = openReader(filePath)) {
            List<Employee> employees = new ArrayList<>();

            String rawLine = br.readLine(); // header
            if (rawLine == null) {
                return employees;
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
                    String prenom = cols[2];
                    int storeId = Integer.parseInt(cols[3]);
                    String poste = cols[4];
                    String departement = cols[5];
                    String dateEmbauche = cols[6];
                    String typeContrat = cols[7];
                    double salaireBrut = Double.parseDouble(cols[8]);

                    employees.add(new Employee(
                            id, nom, prenom, storeId, poste, departement,
                            dateEmbauche, typeContrat, salaireBrut
                    ));
                } catch (NumberFormatException ex) {
                    // ignore ligne invalide
                }
            }

            return employees;
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
}