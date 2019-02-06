package pl.aw84.bdtraining;

import org.apache.avro.Schema;

/**
 * Creates object of Avro domain from CSV field according to field schema
 */
class AvroFieldBuilder {
    private final String input;
    private final Schema.Type type;

    /**
     * @param stringRepresentation Input data from CSV file
     * @param field                Desired output Avro field
     */
    AvroFieldBuilder(String stringRepresentation, Schema.Field field) {
        this.input = stringRepresentation;
        this.type = field.schema().getType();
    }

    AvroFieldBuilder(String stringRepresentation, Schema.Type type) {
        this.input = stringRepresentation;
        this.type = type;
    }

    /**
     * @return Object of type defined in schema
     */
    Object get() {
        if (type == Schema.Type.INT) {
            return Integer.valueOf(input);
        } else if (type == Schema.Type.STRING) {
            return input;
        } else if (type == Schema.Type.DOUBLE) {
            return Double.valueOf(input);
        }
        throw new IllegalStateException("Unknown or unsupported field type");
    }
}
