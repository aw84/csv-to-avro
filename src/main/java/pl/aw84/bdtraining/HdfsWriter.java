package pl.aw84.bdtraining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


/**
 * Class encapsulates creation of HDFS output stream
 */

class HdfsWriter {
    private final OutputStream data_out;

    /**
     * @param outputUri hdfs:// URI to where to write data
     * @throws IOException
     */
    HdfsWriter(String outputUri) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(URI.create(outputUri));
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            throw new IllegalArgumentException("URI already exists, " + outputUri);
        }
        this.data_out = fs.create(new Path(outputUri));
    }

    /**
     * Provides output stream for writing data. It is programmer's responsibility to {@link #close()} this
     * stream in either case successful, error or exception.
     *
     * @return Output stream for writing data
     */
    OutputStream getOutputStream() {
        return this.data_out;
    }

    /**
     * Close output stream
     *
     * @throws IOException
     */
    void close() throws IOException {
        this.data_out.close();
    }
}
