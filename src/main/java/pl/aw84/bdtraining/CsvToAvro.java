package pl.aw84.bdtraining;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.util.List;


/**
 * Class to orchestrate conversion process from CSV to AVRO format
 */

class CsvToAvro {
    private final Schema schema;
    private final HdfsReader hdfsReader;
    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final HdfsWriter hdfsWriter;

    /**
     * @param inputUri  were input CSV is
     * @param outputUri where to put output of conversion
     * @param schema    schema of Avro file
     * @throws IOException
     */
    CsvToAvro(String inputUri, String outputUri, Schema schema) throws IOException {
        this.schema = schema;
        this.hdfsReader = new HdfsReader(inputUri);
        this.hdfsWriter = new HdfsWriter(outputUri);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        this.dataFileWriter = dataFileWriter.create(schema, this.hdfsWriter.getOutputStream());
    }

    /**
     * Executes conversion process
     *
     * @throws IOException
     */
    void convert() throws IOException {
        List<Schema.Field> avroFields = schema.getFields();
        String csvLine;
        try {
            while ((csvLine = hdfsReader.readLine()) != null) {
                GenericRecord record = getRecord(csvLine, avroFields);
                dataFileWriter.append(record);
            }
        } finally {
            hdfsReader.close();
            hdfsWriter.close();
        }
    }

    /**
     * Creates {@link GenericRecord} using provided CSV input
     *
     * @param line One line from CSV file
     * @return GenericRecord built using schema
     */
    private GenericRecord getRecord(String line, List<Schema.Field> avroFields) {
        String[] csvFields = line.split(",");
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < csvFields.length; i++) {
            Object field = new AvroFieldBuilder(csvFields[i], avroFields.get(i)).get();
            record.put(i, field);
        }
        return record;
    }
}
