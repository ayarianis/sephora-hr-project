package com.sephora.data.beam;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmployeeIngestionPipelineTest {

    @Rule
    public final TestPipeline p = TestPipeline.create();

    @Test
    public void testParseEmployeeFullCsv() throws Exception {
        String csvContent =
                "id,nom,prenom,store_id,poste,departement,date_embauche,type_contrat,salaire_brut\n" +
                        "1,Roux,Maxime,1,Stockiste,Logistique,2024-12-21,CDI,23365\n" +
                        "2,Robert,Chloe,1,Assistant(e) RH,RH,2018-07-09,Stage,10992\n";

        Path tempFile = Files.createTempFile("employees_full_test", ".csv");
        Files.writeString(tempFile, csvContent, StandardCharsets.UTF_8);

        PCollection<FileIO.ReadableFile> files = p
                .apply("CreateEmployeePath", Create.of(tempFile.toString()))
                .apply("MatchEmployeeFile", FileIO.matchAll())
                .apply("ReadEmployeeFile", FileIO.readMatches());

        PCollection<TableRow> output = files.apply(
                "ParseEmployeeFull",
                ParDo.of(new EmployeeIngestionPipeline.ParseEmployeeFullCsvDoFn())
        );

        PAssert.that(output).satisfies(rows -> {
            int count = 0;
            for (TableRow row : rows) count++;
            if (count != 2) throw new AssertionError("2 employees attendus");
            return null;
        });

        p.run().waitUntilFinish();
    }

    @Test
    public void testParseEmployeeDeltaJson() throws Exception {
        String jsonContent =
                "[" +
                        "{\"action\":\"INSERT\",\"timestamp\":\"2025-02-01T09:00:00Z\",\"id\":810,\"nom\":\"Dupont\",\"prenom\":\"Sophie\",\"store_id\":1,\"poste\":\"Conseiller(e) Beaute\",\"departement\":\"Vente\",\"date_embauche\":\"2025-01-28\",\"type_contrat\":\"CDI\",\"salaire_brut\":25500}," +
                        "{\"action\":\"DELETE\",\"timestamp\":\"2025-02-01T11:00:00Z\",\"id\":42}" +
                        "]";

        Path tempFile = Files.createTempFile("employees_delta_test", ".json");
        Files.writeString(tempFile, jsonContent, StandardCharsets.UTF_8);

        PCollection<FileIO.ReadableFile> files = p
                .apply("CreateEmployeeDeltaPath", Create.of(tempFile.toString()))
                .apply("MatchEmployeeDeltaFile", FileIO.matchAll())
                .apply("ReadEmployeeDeltaFile", FileIO.readMatches());

        PCollection<TableRow> output = files.apply(
                "ParseEmployeeDelta",
                ParDo.of(new EmployeeIngestionPipeline.ParseEmployeeDeltaJsonDoFn())
        );

        PAssert.that(output).satisfies(rows -> {
            int count = 0;
            boolean foundDelete = false;
            for (TableRow row : rows) {
                count++;
                if ("42".equals(String.valueOf(row.get("id")))) {
                    foundDelete = true;
                }
            }
            if (count != 2) throw new AssertionError("2 lignes delta attendues");
            if (!foundDelete) throw new AssertionError("DELETE non trouvé");
            return null;
        });

        p.run().waitUntilFinish();
    }
}