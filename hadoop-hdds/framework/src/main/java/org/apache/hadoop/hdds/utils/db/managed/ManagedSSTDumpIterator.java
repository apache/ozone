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

import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.eclipse.jetty.io.RuntimeIOException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Iterator to Parse output of RocksDBSSTDumpTool.
 */
public class ManagedSSTDumpIterator implements
        Iterator<ManagedSSTDumpIterator.KeyValue>, AutoCloseable {
  private Process process;
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
  private Lock lock;


  public ManagedSSTDumpIterator(String sstDumptoolJarPath,
                                String sstFilePath,
                                long pollIntervalMillis) throws IOException {
    File sstFile = new File(sstFilePath);
    if (!sstFile.exists() || !sstFile.isFile()) {
      throw new IOException(String.format("Invalid SST File Path : %s",
              sstFile.getAbsolutePath()));
    }
    this.pollIntervalMillis = pollIntervalMillis;
    this.lock = new ReentrantLock();
    init(sstFile, sstDumptoolJarPath);
  }

  private void init(File sstFile, String sstDumptoolJarPath)
          throws IOException {
    List<String> args = Lists.newArrayList(
            "--file=" + sstFile.getAbsolutePath(),
            "--command=scan");
    process = HddsServerUtil.getJavaProcess(Collections.emptyList(),
            sstDumptoolJarPath, SST_DUMP_TOOL_CLASS, args).start();
    processOutput = new BufferedReader(new InputStreamReader(
            process.getInputStream(), StandardCharsets.UTF_8));
    stdoutString = new StringBuilder();
    currentMatcher = PATTERN_MATCHER.matcher(stdoutString);
    charBuffer = new char[8192];
    next();
  }

  private void checkSanityOfProcess() {
    if (!process.isAlive() && process.exitValue() != 0) {
      throw new RuntimeException("Process Terminated with non zero " +
              String.format("exit value %d", process.exitValue()));
    }
  }

  @Override
  public boolean hasNext() {
    checkSanityOfProcess();
    return nextKey != null;
  }

  @Override
  public KeyValue next() throws RuntimeIOException {
    checkSanityOfProcess();
    try {
      lock.lock();
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
              currentKey.setValue(stdoutString.toString());
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
                currentMatcher.start()));
      }
      prevMatchEndIndex = currentMatcher.end();
      nextKey =  new KeyValue(currentMatcher.group(1), currentMatcher.group(2),
              currentMatcher.group(3));
      return currentKey;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if (this.process != null) {
      this.process.destroyForcibly();
      this.processOutput.close();
    }
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
}
