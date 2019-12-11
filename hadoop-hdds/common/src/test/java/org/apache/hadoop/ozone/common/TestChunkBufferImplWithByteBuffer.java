package org.apache.hadoop.ozone.common;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TestChunkBufferImplWithByteBuffer {

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElementException() {
        ChunkBuffer impl = ChunkBuffer.allocate(10);
        Iterator<ByteBuffer> iterator = impl.iterate(1).iterator();
        while (true) {
            iterator.next();
        }
    }
}