package com.sephora.data.beam;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class EmployeeIngestionPipeline {

    public interface EmployeePipelineOptions extends PipelineOptions {
        @Description("Mode: full ou delta")
        String getMode();
        void setMode(String value);

        @Description("Input path")
        String getInputPath();
        void setInputPath(String value);

        @Description("Output table")
        String getOutputTable();
        void setOutputTable(String value);

        @Description("Backup path")
        String getBackupPath();
        void setBackupPath(String value);

        @Description("WRITE_TRUNCATE ou WRITE_APPEND")
        String getWriteDispositionValue();
        void setWriteDispositionValue(String value);
    }

    public static void main(String[] args) {
        EmployeePipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(EmployeePipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> rows;

        if ("full".equalsIgnoreCase(options.getMode())) {
            rows = pipeline
                    .apply("MatchFullCsv", FileIO.match().filepattern(options.getInputPath()))
                    .apply("ReadFullCsv", FileIO.readMatches())
                    .apply("ParseFullCsv", ParDo.of(new ParseEmployeeFullCsvDoFn()));
        } else if ("delta".equalsIgnoreCase(options.getMode())) {
            rows = pipeline
                    .apply("MatchDeltaJson", FileIO.match().filepattern(options.getInputPath()))
                    .apply("ReadDeltaJson", FileIO.readMatches())
                    .apply("ParseDeltaJson", ParDo.of(new ParseEmployeeDeltaJsonDoFn()));
        } else {
            throw new IllegalArgumentException("Mode invalide: " + options.getMode());
        }

        rows.apply("WriteBQ",
                BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(parseWriteDisposition(options.getWriteDispositionValue()))
                        .withSchema(buildSchema())
        );

        rows.apply("ToJsonBackup", ParDo.of(new TableRowToJsonDoFn()))
                .apply("WriteBackup", TextIO.write()
                        .to(options.getBackupPath())
                        .withSuffix(".jsonl")
                        .withoutSharding());

        pipeline.run();
    }

    private static BigQueryIO.Write.WriteDisposition parseWriteDisposition(String value) {
        if ("WRITE_APPEND".equalsIgnoreCase(value)) {
            return BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
        }
        return BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
    }

    public static TableSchema buildSchema() {
        List<TableFieldSchema> fields = Arrays.asList(
                new TableFieldSchema().setName("id").setType("INTEGER"),
                new TableFieldSchema().setName("nom").setType("STRING"),
                new TableFieldSchema().setName("prenom").setType("STRING"),
                new TableFieldSchema().setName("store_id").setType("INTEGER"),
                new TableFieldSchema().setName("poste").setType("STRING"),
                new TableFieldSchema().setName("departement").setType("STRING"),
                new TableFieldSchema().setName("date_embauche").setType("STRING"),
                new TableFieldSchema().setName("type_contrat").setType("STRING"),
                new TableFieldSchema().setName("salaire_brut").setType("FLOAT"),
                new TableFieldSchema().setName("load_mode").setType("STRING"),
                new TableFieldSchema().setName("delta_action").setType("STRING"),
                new TableFieldSchema().setName("event_timestamp").setType("STRING"),
                new TableFieldSchema().setName("is_deleted").setType("BOOLEAN"),
                new TableFieldSchema().setName("ingestion_ts").setType("STRING")
        );
        return new TableSchema().setFields(fields);
    }

    public static class ParseEmployeeFullCsvDoFn extends DoFn<FileIO.ReadableFile, TableRow> {
        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<TableRow> out) throws Exception {
            String content = file.readFullyAsUTF8String();
            String[] lines = content.split("\\R");

            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;

                String[] cols = line.split(",", -1);
                for (int j = 0; j < cols.length; j++) {
                    cols[j] = cols[j].trim().replace("\"", "");
                }

                if (cols.length < 9) continue;

                TableRow row = new TableRow()
                        .set("id", parseInteger(cols[0]))
                        .set("nom", cols[1])
                        .set("prenom", cols[2])
                        .set("store_id", parseInteger(cols[3]))
                        .set("poste", cols[4])
                        .set("departement", cols[5])
                        .set("date_embauche", cols[6])
                        .set("type_contrat", cols[7])
                        .set("salaire_brut", parseDouble(cols[8]))
                        .set("load_mode", "full")
                        .set("delta_action", "FULL")
                        .set("event_timestamp", null)
                        .set("is_deleted", false)
                        .set("ingestion_ts", Instant.now().toString());

                out.output(row);
            }
        }
    }

    public static class ParseEmployeeDeltaJsonDoFn extends DoFn<FileIO.ReadableFile, TableRow> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<TableRow> out) throws Exception {
            String content = file.readFullyAsUTF8String();
            JsonNode root = MAPPER.readTree(content);
            if (!root.isArray()) return;

            for (JsonNode node : root) {
                String action = node.path("action").asText();
                boolean isDeleted = "DELETE".equalsIgnoreCase(action);

                TableRow row = new TableRow()
                        .set("id", nullableInt(node, "id"))
                        .set("nom", nullableText(node, "nom"))
                        .set("prenom", nullableText(node, "prenom"))
                        .set("store_id", nullableInt(node, "store_id"))
                        .set("poste", nullableText(node, "poste"))
                        .set("departement", nullableText(node, "departement"))
                        .set("date_embauche", nullableText(node, "date_embauche"))
                        .set("type_contrat", nullableText(node, "type_contrat"))
                        .set("salaire_brut", nullableDouble(node, "salaire_brut"))
                        .set("load_mode", "delta")
                        .set("delta_action", action)
                        .set("event_timestamp", nullableText(node, "timestamp"))
                        .set("is_deleted", isDeleted)
                        .set("ingestion_ts", Instant.now().toString());

                out.output(row);
            }
        }
    }

    public static class TableRowToJsonDoFn extends DoFn<TableRow, String> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<String> out) throws Exception {
            out.output(MAPPER.writeValueAsString(row));
        }
    }

    private static Integer parseInteger(String value) {
        if (value == null || value.isBlank()) return null;
        return Integer.parseInt(value);
    }

    private static Double parseDouble(String value) {
        if (value == null || value.isBlank()) return null;
        return Double.parseDouble(value);
    }

    private static String nullableText(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asText() : null;
    }

    private static Integer nullableInt(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asInt() : null;
    }

    private static Double nullableDouble(JsonNode node, String field) {
        return node.has(field) && !node.get(field).isNull() ? node.get(field).asDouble() : null;
    }
}