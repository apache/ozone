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

import org.apache.hadoop.util.ClosableIterator;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Iterator to Parse output of RocksDBSSTDumpTool.
 */
public abstract class ManagedSSTDumpIterator<T> implements ClosableIterator<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManagedSSTDumpIterator.class);
  // Since we don't have any restriction on the key, we are prepending
  // the length of the pattern in the sst dump tool output.
  // The first token in the pattern is the key.
  // The second tells the sequence number of the key.
  // The third token gives the type of key in the sst file.
  private static final String PATTERN_REGEX =
      "'([\\s\\S]+)' seq:([0-9]+), type:([0-9]+)";
  public static final int PATTERN_KEY_GROUP_NUMBER = 1;
  public static final int PATTERN_SEQ_GROUP_NUMBER = 2;
  public static final int PATTERN_TYPE_GROUP_NUMBER = 3;
  private static final Pattern PATTERN_MATCHER = Pattern.compile(PATTERN_REGEX);
  private BufferedReader processOutput;
  private KeyValue currentKey;
  private char[] charBuffer;
  private KeyValue nextKey;

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
    StringBuilder value = new StringBuilder();
    int val;
    while ((val = processOutput.read()) != -1) {
      if (val >= '0' && val <= '9') {
        value.append((char) val);
      } else if (value.length() > 0) {
        break;
      }
    }
    return value.length() > 0 ? Optional.of(Integer.valueOf(value.toString()))
        : Optional.empty();
  }

  /**
   * Reads the next n chars from the stream & makes a string.
   *
   * @param numberOfChars
   * @return String of next chars read
   * @throws IOException
   */
  private String readNextNumberOfCharsFromStream(int numberOfChars)
      throws IOException {
    StringBuilder value = new StringBuilder();
    while (numberOfChars > 0) {
      int noOfCharsRead = processOutput.read(charBuffer, 0,
          Math.min(numberOfChars, charBuffer.length));
      if (noOfCharsRead == -1) {
        break;
      }
      value.append(charBuffer, 0, noOfCharsRead);
      numberOfChars -= noOfCharsRead;
    }

    return value.toString();
  }

  private void init(ManagedSSTDumpTool sstDumpTool, File sstFile,
                    ManagedOptions options)
      throws NativeLibraryNotLoadedException {
    String[] args = {"--file=" + sstFile.getAbsolutePath(), "--command=scan"};
    this.sstDumpToolTask = sstDumpTool.run(args, options);
    processOutput = new BufferedReader(new InputStreamReader(
        sstDumpToolTask.getPipedOutput(), StandardCharsets.UTF_8));
    charBuffer = new char[8192];
    open = new AtomicBoolean(true);
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
    return nextKey != null;
  }

  /**
   * Transforms Key to a certain value.
   *
   * @param value
   * @return transformed Value
   */
  protected abstract T getTransformedValue(KeyValue value);

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
    nextKey = null;
    boolean keyFound = false;
    while (!keyFound) {
      try {
        Optional<Integer> keyLength = getNextNumberInStream();
        if (!keyLength.isPresent()) {
          return getTransformedValue(currentKey);
        }
        String keyStr = readNextNumberOfCharsFromStream(keyLength.get());
        Matcher matcher = PATTERN_MATCHER.matcher(keyStr);
        if (keyStr.length() == keyLength.get() && matcher.find()) {
          Optional<Integer> valueLength = getNextNumberInStream();
          if (valueLength.isPresent()) {
            String valueStr = readNextNumberOfCharsFromStream(
                valueLength.get());
            if (valueStr.length() == valueLength.get()) {
              keyFound = true;
              nextKey = new KeyValue(matcher.group(PATTERN_KEY_GROUP_NUMBER),
                  matcher.group(PATTERN_SEQ_GROUP_NUMBER),
                  matcher.group(PATTERN_TYPE_GROUP_NUMBER),
                  valueStr);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
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
    private String key;
    private Integer sequence;
    private Integer type;

    private String value;

    private KeyValue(String key, String sequence, String type,
                     String value) {
      this.key = key;
      this.sequence = Integer.valueOf(sequence);
      this.type = Integer.valueOf(type);
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public Integer getSequence() {
      return sequence;
    }

    public Integer getType() {
      return type;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "KeyValue{" + "key='" + key + '\'' + ", sequence=" + sequence +
          ", type=" + type + ", value='" + value + '\'' + '}';
    }
  }
}
