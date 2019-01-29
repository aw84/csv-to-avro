package pl.aw84.bdtraining;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.List;

class CsvToAvro {
    private final Schema schema;
    private final String csvFilename;
    private final String outputFilename;
    private final FileSystem fs;
    private final Configuration conf;

    CsvToAvro(String csvFilename, String outputFilename, Schema schema) throws IOException {
        this.csvFilename = csvFilename;
        this.outputFilename = outputFilename;
        this.schema = schema;
        this.conf = new Configuration();
        this.fs = FileSystem.get(URI.create(csvFilename), conf);
    }

    private String readLine() throws IOException {
        FSDataInputStream data_in = null;
        Reader reader = null;
        try {
            data_in = fs.open(new Path(csvFilename));
            reader = new InputStreamReader(data_in, "UTF-8");
            BufferedReader buf = new BufferedReader(reader);
            String line;
            while( (line = buf.readLine()) != null) {
                GenericRecord record = getRecord(line);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
            data_in.close();
        }
        return null;
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
