package pl.aw84.bdtraining;

import training.avro.Entity;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        String csvFilename = args[0];
        String outputFilename = args[1];
        try {
            CsvToAvro csvToAvro = new CsvToAvro(csvFilename, outputFilename, new Entity().getSchema());
            csvToAvro.convert();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
