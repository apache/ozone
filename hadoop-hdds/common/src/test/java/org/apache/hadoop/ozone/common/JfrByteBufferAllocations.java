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

package org.apache.hadoop.ozone.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads jdk.ObjectAllocationInNewTLAB ByteBuffer allocations via JFR reflection
 * so test compilation does not require jdk.jfr on the compile classpath.
 */
final class JfrByteBufferAllocations {

  private static final boolean JFR_AVAILABLE = probeJfr();

  private JfrByteBufferAllocations() {
  }

  static boolean isAvailable() {
    return JFR_AVAILABLE;
  }

  static AllocationStats measure(Runnable workload) throws IOException {
    if (!JFR_AVAILABLE) {
      workload.run();
      return new AllocationStats(0, 0);
    }
    try {
      final Class<?> recordingClass = Class.forName("jdk.jfr.Recording");
      final Object recording = recordingClass.getConstructor().newInstance();
      final Object eventSettings = recordingClass.getMethod("enable", String.class)
          .invoke(recording, "jdk.ObjectAllocationInNewTLAB");
      eventSettings.getClass().getMethod("withoutStackTrace").invoke(eventSettings);
      recordingClass.getMethod("start").invoke(recording);
      try {
        workload.run();
      } finally {
        recordingClass.getMethod("stop").invoke(recording);
      }
      return readByteBufferAllocations(recording);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Failed to run JFR allocation measurement", e);
    }
  }

  private static AllocationStats readByteBufferAllocations(Object recording)
      throws IOException, ReflectiveOperationException {
    final Path dump = Files.createTempFile("chunkbuffer-put-bench", ".jfr");
    try {
      recording.getClass().getMethod("dump", Path.class).invoke(recording, dump);
      final Class<?> recordingFileClass = Class.forName("jdk.jfr.consumer.RecordingFile");
      final Object recordingFile = recordingFileClass.getConstructor(Path.class).newInstance(dump);
      final java.lang.reflect.Method hasMoreEvents = recordingFileClass.getMethod("hasMoreEvents");
      final java.lang.reflect.Method readEvent = recordingFileClass.getMethod("readEvent");
      final java.lang.reflect.Method close = recordingFileClass.getMethod("close");

      long byteBufferAllocCount = 0;
      long byteBufferAllocBytes = 0;
      try {
        while ((Boolean) hasMoreEvents.invoke(recordingFile)) {
          final Object event = readEvent.invoke(recordingFile);
          final Class<?> eventClass = event.getClass();
          final Object eventType = eventClass.getMethod("getEventType").invoke(event);
          final String eventName = (String) eventType.getClass().getMethod("getName")
              .invoke(eventType);
          if (!"jdk.ObjectAllocationInNewTLAB".equals(eventName)) {
            continue;
          }
          final Object objectClass = eventClass.getMethod("getValue", String.class)
              .invoke(event, "objectClass");
          if (objectClass == null) {
            continue;
          }
          final String className = (String) objectClass.getClass().getMethod("getName")
              .invoke(objectClass);
          if (className.contains("ByteBuffer")) {
            byteBufferAllocCount++;
            byteBufferAllocBytes += (Long) eventClass.getMethod("getLong", String.class)
                .invoke(event, "allocationSize");
          }
        }
      } finally {
        close.invoke(recordingFile);
      }
      return new AllocationStats(byteBufferAllocCount, byteBufferAllocBytes);
    } finally {
      Files.deleteIfExists(dump);
    }
  }

  private static boolean probeJfr() {
    try {
      final Class<?> frClass = Class.forName("jdk.jfr.FlightRecorder");
      return (Boolean) frClass.getMethod("isAvailable").invoke(null);
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }

  static final class AllocationStats {
    private final long byteBufferAllocCount;
    private final long byteBufferAllocBytes;

    private AllocationStats(long byteBufferAllocCount, long byteBufferAllocBytes) {
      this.byteBufferAllocCount = byteBufferAllocCount;
      this.byteBufferAllocBytes = byteBufferAllocBytes;
    }

    long getByteBufferAllocCount() {
      return byteBufferAllocCount;
    }

    long getByteBufferAllocBytes() {
      return byteBufferAllocBytes;
    }
  }
}
