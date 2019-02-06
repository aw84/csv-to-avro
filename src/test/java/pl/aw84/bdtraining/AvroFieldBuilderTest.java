package pl.aw84.bdtraining;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class AvroFieldBuilderTest {

    @Test
    public void getInteger() {
        AvroFieldBuilder testObject = new AvroFieldBuilder("1", Schema.Type.INT);
        Object o = testObject.get();
        Assert.assertTrue(o instanceof Integer);
    }

    @Test
    public void getString() {
        AvroFieldBuilder testObject = new AvroFieldBuilder(" ", Schema.Type.STRING);
        Object o = testObject.get();
        Assert.assertTrue(o instanceof String);
    }

    @Test
    public void getDouble() {
        AvroFieldBuilder testObject = new AvroFieldBuilder("9.333", Schema.Type.DOUBLE);
        Object o = testObject.get();
        Assert.assertTrue(o instanceof Double);
    }
}