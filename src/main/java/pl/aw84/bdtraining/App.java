package pl.aw84.bdtraining;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import training.avro.Entity;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String csvFilename = "train.csv";
        String outputFilename = "train.avro";
        CsvToAvro csvToAvro = new CsvToAvro(csvFilename, outputFilename, new Entity().getSchema());
        csvToAvro.convert();
        GenericRecord record = new GenericData.Record(new Entity().getSchema());
        record.put(idx, csvValue);
    }
}
