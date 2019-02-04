package pl.aw84.bdtraining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


/**
 * Provides functionality of reading URI from HDFS on line-by-line basis
 */

class HdfsReader {
    private final FSDataInputStream data_in;
    private final BufferedReader buf;

    /**
     * It's programmes responsibility to call {@link #close()} when done reading.
     * Either successful or in case of failure or exception.
     *
     * @param uri URI pointing what to read
     * @throws IOException
     */
    HdfsReader(String uri) throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        this.data_in = fs.open(new Path(uri));

        InputStreamReader reader = new InputStreamReader(data_in, "UTF-8");
        this.buf = new BufferedReader(reader);
    }

    String readLine() throws IOException {
        return buf.readLine();
    }

    void close() throws IOException {
        this.data_in.close();
    }
}
