package pl.aw84.bdtraining;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.util.List;

class CsvToAvro {
    private final Schema schema;
    private final HdfsReader hdfsReader;
    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final HdfsWriter hdfsWriter;

    CsvToAvro(String csvInputUri, String outputUri, Schema schema) throws IOException {
        this.schema = schema;

        this.hdfsReader = new HdfsReader(csvInputUri);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        this.hdfsWriter = new HdfsWriter(outputUri);

        dataFileWriter.create(schema, this.hdfsWriter.getOutputStream());
        this.dataFileWriter = dataFileWriter;
    }

    void convert() throws IOException {
        String csvLine;
        try {
            while ((csvLine = hdfsReader.readLine()) != null) {
                GenericRecord record = getRecord(csvLine);
                dataFileWriter.append(record);
            }
        } finally {
            hdfsReader.close();
            hdfsWriter.close();
        }
    }

    private Object getField(String csvField, Schema.Field field) {
        if (field.schema().getType() == Type.INT) {
            return Integer.valueOf(csvField);
        } else if (field.schema().getType() == Type.STRING) {
            return csvField;
        }
        return null;
    }

    GenericRecord getRecord(String csvLine) {
        String[] csvFields = csvLine.split(",");
        GenericRecord record = new GenericData.Record(schema);
        List<Schema.Field> avroFields = schema.getFields();
        for (int i = 0; i < csvFields.length; i++) {
            record.put(i, getField(csvFields[i], avroFields.get(i)));
        }
        return record;
    }
}
