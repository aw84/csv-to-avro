package pl.aw84.bdtraining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

class HdfsWriter {
    private final String uri;
    private final Configuration conf;
    private final FileSystem fs;
    private final OutputStream data_out;


    HdfsWriter(String outputUri) throws IOException {
        this.uri = outputUri;
        this.conf = new Configuration();

        this.fs = FileSystem.get(URI.create(uri), conf);
        this.data_out = fs.create(new Path(uri));
    }

    OutputStream getOutputStream() {
        return this.data_out;
    }

    void close() throws IOException {
        this.data_out.close();
    }
}
