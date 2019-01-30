package pl.aw84.bdtraining;

import training.avro.Entity;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String csvFilename = "train.csv";
        String outputFilename = "train.avro";
        try {
            CsvToAvro csvToAvro = new CsvToAvro(csvFilename, outputFilename, new Entity().getSchema());
            csvToAvro.convert();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
