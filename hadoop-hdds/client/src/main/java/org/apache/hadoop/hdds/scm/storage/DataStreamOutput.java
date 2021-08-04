package org.apache.hadoop.hdds.scm.storage;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

public interface DataStreamOutput extends Closeable {

    void write(ByteBuf b) throws IOException;

    default void write(ByteBuf b, int off, int len) throws IOException {
        write(b.slice(off, len));
    }

    void flush() throws IOException;
}
