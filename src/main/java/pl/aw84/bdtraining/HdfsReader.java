package pl.aw84.bdtraining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class HdfsReader {
    private final String uri;
    private final Configuration conf;
    private final FileSystem fs;
    private final FSDataInputStream data_in;
    private final InputStreamReader reader;
    private final BufferedReader buf;

    HdfsReader(String uri) throws IOException {
        this.uri = uri;
        this.conf = new Configuration();

        this.fs = FileSystem.get(URI.create(uri), conf);
        this.data_in = fs.open(new Path(uri));

        this.reader = new InputStreamReader(data_in, "UTF-8");
        this.buf = new BufferedReader(reader);
    }

    String readLine() throws IOException {
        return buf.readLine();
    }

    void close() throws IOException {
        this.data_in.close();
    }
}
