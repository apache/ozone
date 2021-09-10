package org.apache.hadoop.hdds.scm.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.scm.storage.ByteArrayReader;
import org.apache.hadoop.hdds.scm.storage.ByteBufferReader;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class ExtendedInputStream extends InputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  protected static final int EOF = -1;
  protected long position = 0;

  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    ByteReaderStrategy strategy = new ByteArrayReader(b, off, len);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    ByteReaderStrategy strategy = new ByteBufferReader(byteBuffer);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  /**
   * This must be overridden by the extending classes to call read on the
   * underlying stream they are reading from. The last stream in the chain (the
   * one which provides the actual data) needs to provide a real read via the
   * read methods. For example if a test is extending this class, then it will
   * need to override both read methods above and provide a dummy
   * readWithStrategy implementation, as it will never be called by the tests.
   *
   * @param strategy
   * @return
   * @throws IOException
   */
  protected abstract int readWithStrategy(ByteReaderStrategy strategy) throws
      IOException;

  @Override
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized void seek(long l) throws IOException {
    throw new NotImplementedException("Seek is not implements for EC");
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }
}