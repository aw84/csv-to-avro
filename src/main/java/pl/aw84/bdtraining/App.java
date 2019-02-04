package pl.aw84.bdtraining;

import training.avro.Entity;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Provide one source URI and one output URI on command line");
            System.err.println("Example:  hadoop jar csv-to-avro-1.0-SNAPSHOT.jar pl.aw84.bdtraining.App hdfs:///INPUT.csv hdfs:///OUTPUT.avro");
            System.exit(-1);
        }
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
