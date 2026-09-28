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

package org.apache.hadoop.hdds.scm.simulation;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Records every step of a simulation run. Two runs with the same seed and settings must record identical traces;
 * {@link #hash()} is used to check it.
 */
final class SimTrace {

  private final MessageDigest digest;
  private final Deque<String> recent = new ArrayDeque<>();
  private final int recentLimit;
  private final List<String> all;
  private long lines;

  SimTrace(int recentLimit, boolean keepAll) {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    this.recentLimit = recentLimit;
    this.all = keepAll ? new ArrayList<>() : null;
  }

  void record(long elapsedMs, String kind, String detail) {
    String line = elapsedMs + " " + kind + " " + detail;
    digest.update(line.getBytes(UTF_8));
    digest.update((byte) '\n');
    lines++;
    recent.addLast(line);
    if (recent.size() > recentLimit) {
      recent.removeFirst();
    }
    if (all != null) {
      all.add(line);
    }
  }

  long size() {
    return lines;
  }

  /** Hash of all lines recorded so far. */
  String hash() {
    try {
      byte[] bytes = ((MessageDigest) digest.clone()).digest();
      StringBuilder sb = new StringBuilder();
      for (byte b : bytes) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  List<String> recentLines() {
    return new ArrayList<>(recent);
  }

  /** Writes the full trace if it was kept, otherwise the recent lines. */
  void writeTo(Path file) throws IOException {
    Files.createDirectories(file.getParent());
    Files.write(file, all != null ? all : recentLines(), UTF_8);
  }
}
