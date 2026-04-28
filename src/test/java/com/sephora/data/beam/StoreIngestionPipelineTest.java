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

public class StoreIngestionPipelineTest {

    @Rule
    public final TestPipeline p = TestPipeline.create();

    @Test
    public void testParseStoreFullCsv() throws Exception {
        String csvContent =
                "id,nom,adresse,pays,region,surface,date_ouverture,statut,nb_employes\n" +
                        "1,Store A,Addr A,FR,IDF,100,2020-01-01,OUVERT,10\n" +
                        "2,Store B,Addr B,US,CA,120,2021-01-01,OUVERT,8\n";

        Path tempFile = Files.createTempFile("stores_full_test", ".csv");
        Files.writeString(tempFile, csvContent, StandardCharsets.UTF_8);

        PCollection<FileIO.ReadableFile> files = p
                .apply("CreateStorePath", Create.of(tempFile.toString()))
                .apply("MatchStoreFile", FileIO.matchAll())
                .apply("ReadStoreFile", FileIO.readMatches());

        PCollection<TableRow> output = files.apply(
                "ParseStoreFull",
                ParDo.of(new StoreIngestionPipeline.ParseStoreFullCsvDoFn())
        );

        PAssert.that(output).satisfies(rows -> {
            int count = 0;
            for (TableRow row : rows) count++;
            if (count != 2) throw new AssertionError("2 stores attendus");
            return null;
        });

        p.run().waitUntilFinish();
    }
}