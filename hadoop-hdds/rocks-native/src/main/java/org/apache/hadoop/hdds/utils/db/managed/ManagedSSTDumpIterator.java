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

import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.eclipse.jetty.io.RuntimeIOException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Iterator to Parse output of RocksDBSSTDumpTool.
 */
public class ManagedSSTDumpIterator implements
        Iterator<ManagedSSTDumpIterator.KeyValue>, AutoCloseable {
  private static final String SST_DUMP_TOOL_CLASS =
          "org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool";
  private static final String PATTERN_REGEX =
          "'([^=>]+)' seq:([0-9]+), type:([0-9]+) =>";

  public static final int PATTERN_KEY_GROUP_NUMBER = 1;
  public static final int PATTERN_SEQ_GROUP_NUMBER = 2;
  public static final int PATTERN_TYPE_GROUP_NUMBER = 3;
  private static final Pattern PATTERN_MATCHER =
          Pattern.compile(PATTERN_REGEX);
  private BufferedReader processOutput;
  private StringBuilder stdoutString;

  private Matcher currentMatcher;
  private int prevMatchEndIndex;
  private KeyValue currentKey;
  private char[] charBuffer;
  private KeyValue nextKey;

  private long pollIntervalMillis;
  private ManagedSSTDumpTool.SSTDumpToolTask sstDumpToolTask;
  private Lock lock;
  private AtomicBoolean open;


  public ManagedSSTDumpIterator(ManagedSSTDumpTool sstDumpTool,
                                String sstFilePath,
                                ManagedOptions options,
                                long pollIntervalMillis) throws IOException,
          NativeLibraryNotLoadedException {
    File sstFile = new File(sstFilePath);
    if (!sstFile.exists() || !sstFile.isFile()) {
      throw new IOException(String.format("Invalid SST File Path : %s",
              sstFile.getAbsolutePath()));
    }
    this.pollIntervalMillis = pollIntervalMillis;
    this.lock = new ReentrantLock();
    init(sstDumpTool, sstFile, options);
  }

  private void init(ManagedSSTDumpTool sstDumpTool, File sstFile,
                    ManagedOptions options)
          throws NativeLibraryNotLoadedException {
    String[] args = {"--file=" + sstFile.getAbsolutePath(),
                     "--command=scan"};
    this.sstDumpToolTask = sstDumpTool.run(args, options);
    processOutput = new BufferedReader(new InputStreamReader(
            sstDumpToolTask.getPipedOutput(), StandardCharsets.UTF_8));
    stdoutString = new StringBuilder();
    currentMatcher = PATTERN_MATCHER.matcher(stdoutString);
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
    if (sstDumpToolTask.getFuture().isDone()
            && sstDumpToolTask.exitValue() != 0) {
      throw new RuntimeException("Process Terminated with non zero " +
              String.format("exit value %d", sstDumpToolTask.exitValue()));
    }
  }

  /**
   *
   * @return
   * Throws Runtime Exception in case of SST File read failure
   */

  @Override
  public boolean hasNext() {
    checkSanityOfProcess();
    return nextKey != null;
  }

  /**
   *
   * @return next Key
   * Throws Runtime Exception incase of failure.
   */
  @Override
  public KeyValue next() {
    lock.lock();
    try {
      checkSanityOfProcess();
      currentKey = nextKey;
      nextKey = null;
      while (!currentMatcher.find()) {
        try {
          if (prevMatchEndIndex != 0) {
            stdoutString = new StringBuilder(stdoutString.substring(
                    prevMatchEndIndex, stdoutString.length()));
            prevMatchEndIndex = 0;
            currentMatcher = PATTERN_MATCHER.matcher(stdoutString);
          }
          Thread.sleep(pollIntervalMillis);
          int numberOfCharsRead = processOutput.read(charBuffer);
          if (numberOfCharsRead < 0) {
            if (currentKey != null) {
              currentKey.setValue(stdoutString.substring(0,
                      Math.max(stdoutString.length() -1, 0)));
            }
            return currentKey;
          }
          stdoutString.append(charBuffer, 0, numberOfCharsRead);
          currentMatcher.reset();
        } catch (IOException | InterruptedException e) {
          throw new RuntimeIOException(e);
        }
      }
      if (currentKey != null) {
        currentKey.setValue(stdoutString.substring(prevMatchEndIndex,
                currentMatcher.start() -1));
      }
      prevMatchEndIndex = currentMatcher.end();
      nextKey =  new KeyValue(
              currentMatcher.group(PATTERN_KEY_GROUP_NUMBER),
              currentMatcher.group(PATTERN_SEQ_GROUP_NUMBER),
              currentMatcher.group(PATTERN_TYPE_GROUP_NUMBER));
      return currentKey;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void close() throws Exception {
    lock.lock();
    try {
      if (this.sstDumpToolTask != null) {
        if (!this.sstDumpToolTask.getFuture().isDone()) {
          this.sstDumpToolTask.getFuture().cancel(true);
        }
        this.processOutput.close();
      }
      open.compareAndSet(true, false);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void finalize() throws Throwable {
    this.close();
  }

  /**
   * Class containing Parsed KeyValue Record from Sst Dumptool output.
   */
  public static final class KeyValue {
    private String key;
    private Integer sequence;
    private Integer type;

    private String value;

    private KeyValue(String key, String sequence, String type) {
      this.key = key;
      this.sequence = Integer.valueOf(sequence);
      this.type = Integer.valueOf(type);
    }

    private void setValue(String value) {
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
      return "KeyValue{" +
              "key='" + key + '\'' +
              ", sequence=" + sequence +
              ", type=" + type +
              ", value='" + value + '\'' +
              '}';
    }
  }

  public static void main(String[] args) throws NativeLibraryNotLoadedException, IOException {
    ManagedSSTDumpTool sstDumpTool =
            new ManagedSSTDumpTool(new ForkJoinPool(), 50);
    try (ManagedOptions options = new ManagedOptions();
         ManagedSSTDumpIterator iterator = new ManagedSSTDumpIterator(sstDumpTool,
                 "/Users/sbalachandran/Documents/code/dummyrocks/rocks/000025.sst", options, 2000);
    ) {
      while (iterator.hasNext()) {
        System.out.println(iterator.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
