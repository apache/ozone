/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.io.ByteBufferPool;

/**
 * Input stream which wraps a ECBlockReconstructedStripeInputStream to allow
 * a EC Block to be read via the traditional InputStream read methods.
 */
public class ECBlockReconstructedInputStream extends BlockExtendedInputStream {

  private ECReplicationConfig repConfig;
  private ECBlockReconstructedStripeInputStream stripeReader;
  private ByteBuffer[] bufs;
  private final ByteBufferPool byteBufferPool;
  private boolean closed = false;
  private boolean unBuffered = false;

  private long position = 0;

  public ECBlockReconstructedInputStream(ECReplicationConfig repConfig,
      ByteBufferPool byteBufferPool,
      ECBlockReconstructedStripeInputStream stripeReader) {
    this.repConfig = repConfig;
    this.byteBufferPool = byteBufferPool;
    this.stripeReader = stripeReader;
  }

  @Override
  public synchronized BlockID getBlockID() {
    return stripeReader.getBlockID();
  }

  @Override
  public synchronized long getRemaining() {
    return getLength() - position;
  }

  @Override
  public synchronized long getLength() {
    return stripeReader.getLength();
  }

  @Override
  public synchronized int read(byte[] b, int off, int len)
      throws IOException {
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    ensureNotClosed();
    if (!hasRemaining()) {
      return EOF;
    }
    allocateBuffers();
    if (unBuffered) {
      seek(getPos());
      unBuffered = false;
    }
    int totalRead = 0;
    while (buf.hasRemaining() && getRemaining() > 0) {
      ByteBuffer b = selectNextBuffer();
      if (b == null) {
        // This should not happen, so if it does abort.
        throw new IOException(getRemaining() + " bytes remaining but unable " +
            "to select a buffer with data");
      }
      long read = readBufferToDest(b, buf);
      totalRead += read;
    }
    if (!hasRemaining()) {
      // We have reached the end of the block. While the block is still open
      // and could be seeked back, it is most likely the block will be closed.
      // KeyInputStream does not call close on the block until all blocks in the
      // key have been read, so releasing the resources here helps to avoid
      // excessive memory usage.
      freeBuffers();
    }
    return totalRead;
  }

  private void ensureNotClosed() throws IOException {
    if (closed) {
      throw new IOException("The input stream is closed");
    }
  }

  private ByteBuffer selectNextBuffer() throws IOException {
    for (ByteBuffer b : bufs) {
      if (b.hasRemaining()) {
        return b;
      }
    }
    // If we get here, then no buffer has any remaining, so we need to
    // fill them.
    long read = readStripe();
    if (read == EOF) {
      return null;
    }
    return selectNextBuffer();
  }

  private long readBufferToDest(ByteBuffer src, ByteBuffer dest) {
    int initialRemaining = dest.remaining();
    while (dest.hasRemaining() && src.hasRemaining()) {
      dest.put(src.get());
    }
    int read = initialRemaining - dest.remaining();
    position += read;
    return read;
  }

  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    throw new IOException("Not Implemented");
  }

  @Override
  public synchronized void unbuffer() {
    stripeReader.unbuffer();
    freeBuffers();
    unBuffered = true;
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized void close() throws IOException {
    stripeReader.close();
    freeBuffers();
    closed = true;
  }

  private void freeBuffers() {
    if (bufs != null) {
      for (int i = 0; i < bufs.length; i++) {
        byteBufferPool.putBuffer(bufs[i]);
        bufs[i] = null;
      }
      bufs = null;
    }
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    ensureNotClosed();
    if (pos < 0 || pos > getLength()) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for block: " + getBlockID());
    }
    long stripeSize = (long)repConfig.getEcChunkSize() * repConfig.getData();
    long stripeNum = pos / stripeSize;
    int partial = (int)(pos % stripeSize);
    // Seek the stripe reader to the beginning of the new current stripe
    stripeReader.seek(stripeNum * stripeSize);
    // Now reload the data buffers and adjust their position to the partial
    // stripe offset.
    readAndSeekStripe(partial);
    position = pos;
  }

  private void readAndSeekStripe(int offset) throws IOException {
    allocateBuffers();
    readStripe();
    if (offset == 0) {
      return;
    }
    for (ByteBuffer b : bufs) {
      int newPos = Math.min(b.remaining(), offset);
      b.position(newPos);
      offset -= newPos;
      if (offset == 0) {
        break;
      }
    }
  }

  private long readStripe() throws IOException {
    clearBuffers();
    return stripeReader.readStripe(bufs);
  }

  private void allocateBuffers() {
    if (bufs != null) {
      return;
    }
    bufs = new ByteBuffer[repConfig.getData()];
    for (int i = 0; i < repConfig.getData(); i++) {
      bufs[i] = byteBufferPool.getBuffer(false, repConfig.getEcChunkSize());
      // Initially set the limit to 0 so there is no remaining space.
      bufs[i].limit(0);
    }
  }

  private void clearBuffers() {
    for (ByteBuffer b : bufs) {
      b.clear();
      // As we are getting buffers from a bufferPool, we may get buffers with a
      // capacity larger than what we asked for. After calling clear(), the
      // buffer limit will become the capacity so we need to reset it back to
      // the desired limit.
      b.limit(repConfig.getEcChunkSize());
    }
  }

  private boolean hasRemaining() {
    return getRemaining() > 0;
  }
}
