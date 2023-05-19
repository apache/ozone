/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.utils.db.managed;

import com.google.common.primitives.UnsignedLong;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Iterator to Parse output of RocksDBSSTDumpTool.
 */
public abstract class ManagedSSTDumpIterator<T> implements ClosableIterator<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManagedSSTDumpIterator.class);
  // Since we don't have any restriction on the key & value, we are prepending
  // the length of the pattern in the sst dump tool output.
  // The first token in the pattern is the key.
  // The second tells the sequence number of the key.
  // The third token gives the type of key in the sst file.
  // The fourth token
  private InputStream processOutput;
  private Optional<KeyValue> currentKey;
  private byte[] intBuffer;
  private Optional<KeyValue> nextKey;

  private ManagedSSTDumpTool.SSTDumpToolTask sstDumpToolTask;
  private AtomicBoolean open;
  private StackTraceElement[] stackTrace;


  public ManagedSSTDumpIterator(ManagedSSTDumpTool sstDumpTool,
                                String sstFilePath, ManagedOptions options)
      throws IOException, NativeLibraryNotLoadedException {
    File sstFile = new File(sstFilePath);
    if (!sstFile.exists()) {
      throw new IOException(String.format("File in path : %s doesn't exist",
          sstFile.getAbsolutePath()));
    }
    if (!sstFile.isFile()) {
      throw new IOException(String.format("Path given: %s is not a file",
          sstFile.getAbsolutePath()));
    }
    init(sstDumpTool, sstFile, options);
    this.stackTrace = Thread.currentThread().getStackTrace();
  }

  /**
   * Parses next occuring number in the stream.
   *
   * @return Optional of the integer empty if no integer exists
   */
  private Optional<Integer> getNextNumberInStream() throws IOException {
    int n = processOutput.read(intBuffer, 0, 4);
    if (n == 4) {
      return Optional.of(ByteBuffer.wrap(intBuffer).getInt());
    } else if (n >= 0) {
      throw new IllegalStateException(String.format("Integer expects " +
          "4 bytes to be read from the stream, but read only %d bytes", n));
    }
    return Optional.empty();
  }

  private Optional<byte[]> getNextByteArray() throws IOException {
    Optional<Integer> size = getNextNumberInStream();
    if (size.isPresent()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Allocating byte array, size: {}", size.get());
      }
      byte[] b = new byte[size.get()];
      int n = processOutput.read(b);
      if (n >= 0 && n != size.get()) {
        throw new IllegalStateException(String.format("Integer expects " +
            "4 bytes to be read from the stream, but read only %d bytes", n));
      }
      return Optional.of(b);
    }
    return Optional.empty();
  }

  private Optional<UnsignedLong> getNextUnsignedLong() throws IOException {
    long val = 0;
    for (int i = 0; i < 8; i++) {
      val = val << 8;
      int nextByte = processOutput.read();
      if (nextByte < 0) {
        if (i == 0) {
          return Optional.empty();
        }
        throw new IllegalStateException(String.format("Long expects " +
            "8 bytes to be read from the stream, but read only %d bytes", i));
      }
      val += nextByte;
    }
    return Optional.of(UnsignedLong.fromLongBits(val));
  }

  private void init(ManagedSSTDumpTool sstDumpTool, File sstFile,
                    ManagedOptions options)
      throws NativeLibraryNotLoadedException {
    String[] args = {"--file=" + sstFile.getAbsolutePath(), "--command=scan",
        "--silent"};
    this.sstDumpToolTask = sstDumpTool.run(args, options);
    processOutput = sstDumpToolTask.getPipedOutput();
    intBuffer = new byte[4];
    open = new AtomicBoolean(true);
    currentKey = Optional.empty();
    nextKey = Optional.empty();
    next();
  }

  /**
   * Throws Runtime exception in the case iterator is closed or
   * the native Dumptool exited with non zero exit value.
   */
  private void checkSanityOfProcess() {
    if (!this.open.get()) {
      throw new RuntimeException("Iterator has been closed");
    }
    if (sstDumpToolTask.getFuture().isDone() &&
        sstDumpToolTask.exitValue() != 0) {
      throw new RuntimeException("Process Terminated with non zero " +
          String.format("exit value %d", sstDumpToolTask.exitValue()));
    }
  }

  /**
   * Checks the status of the process & sees if there is another record.
   *
   * @return True if next exists & false otherwise
   * Throws Runtime Exception in case of SST File read failure
   */

  @Override
  public boolean hasNext() {
    checkSanityOfProcess();
    return nextKey.isPresent();
  }

  /**
   * Transforms Key to a certain value.
   *
   * @param value
   * @return transformed Value
   */
  protected abstract T getTransformedValue(Optional<KeyValue> value);

  /**
   * Returns the next record from SSTDumpTool.
   *
   * @return next Key
   * Throws Runtime Exception incase of failure.
   */
  @Override
  public T next() {
    checkSanityOfProcess();
    currentKey = nextKey;
    nextKey = Optional.empty();
    try {
      Optional<byte[]> key = getNextByteArray();
      if (!key.isPresent()) {
        return getTransformedValue(currentKey);
      }
      UnsignedLong sequenceNumber = getNextUnsignedLong()
          .orElseThrow(() -> new IllegalStateException(
              String.format("Error while trying to read sequence number" +
                      " for key %s", StringUtils.bytes2String(key.get()))));

      Integer type = getNextNumberInStream()
          .orElseThrow(() -> new IllegalStateException(
              String.format("Error while trying to read sequence number for " +
                      "key %s with sequence number %s",
                  StringUtils.bytes2String(key.get()),
                  sequenceNumber.toString())));
      byte[] val = getNextByteArray().orElseThrow(() ->
          new IllegalStateException(
              String.format("Error while trying to read sequence number for " +
                      "key %s with sequence number %s of type %d",
                  StringUtils.bytes2String(key.get()),
                  sequenceNumber.toString(), type)));
      nextKey = Optional.of(new KeyValue(key.get(), sequenceNumber, type, val));
    } catch (IOException e) {
      // TODO [SNAPSHOT] Throw custom snapshot exception
      throw new RuntimeIOException(e);
    }
    return getTransformedValue(currentKey);
  }

  @Override
  public synchronized void close() throws UncheckedIOException {
    if (this.sstDumpToolTask != null) {
      if (!this.sstDumpToolTask.getFuture().isDone()) {
        this.sstDumpToolTask.getFuture().cancel(true);
      }
      try {
        this.processOutput.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    open.compareAndSet(true, false);
  }

  @Override
  protected void finalize() throws Throwable {
    if (open.get()) {
      LOG.warn("{}  is not closed properly." +
              " StackTrace for unclosed instance: {}",
          this.getClass().getName(),
          Arrays.stream(stackTrace)
              .map(StackTraceElement::toString).collect(
                  Collectors.joining("\n")));
    }
    this.close();
    super.finalize();
  }

  /**
   * Class containing Parsed KeyValue Record from Sst Dumptool output.
   */
  public static final class KeyValue {

    private final byte[] key;
    private final UnsignedLong sequence;
    private final Integer type;
    private final byte[] value;

    private KeyValue(byte[] key, UnsignedLong sequence, Integer type,
             byte[] value) {
      this.key = key;
      this.sequence = sequence;
      this.type = type;
      this.value = value;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getKey() {
      return key;
    }

    public UnsignedLong getSequence() {
      return sequence;
    }

    public Integer getType() {
      return type;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "KeyValue{" +
          "key=" + StringUtils.bytes2String(key) +
          ", sequence=" + sequence +
          ", type=" + type +
          ", value=" + StringUtils.bytes2String(value) +
          '}';
    }
  }
}
