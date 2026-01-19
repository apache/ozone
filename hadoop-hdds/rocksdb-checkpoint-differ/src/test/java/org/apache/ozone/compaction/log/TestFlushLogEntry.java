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

package org.apache.ozone.compaction.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.FlushLogEntryProto;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for FlushLogEntry.
 */
public class TestFlushLogEntry {

  private static Stream<Arguments> flushLogEntryValidScenarios() {
    FlushFileInfo fileInfo1 = new FlushFileInfo.Builder("000123.sst")
        .setStartRange("key1")
        .setEndRange("key999")
        .setColumnFamily("keyTable")
        .build();

    FlushFileInfo fileInfo2 = new FlushFileInfo.Builder("000456.sst")
        .setStartRange("dir1")
        .setEndRange("dir999")
        .setColumnFamily("directoryTable")
        .build();

    return Stream.of(
        Arguments.of("With flush reason.",
            1000L,
            Time.now(),
            fileInfo1,
            "Manual flush triggered"
        ),
        Arguments.of("Without flush reason.",
            2000L,
            Time.now(),
            fileInfo2,
            null
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushLogEntryValidScenarios")
  public void testGetProtobuf(
      String description,
      long dbSequenceNumber,
      long flushTime,
      FlushFileInfo fileInfo,
      String flushReason) {
    FlushLogEntry.Builder builder = new FlushLogEntry
        .Builder(dbSequenceNumber, flushTime, fileInfo);

    if (flushReason != null) {
      builder.setFlushReason(flushReason);
    }

    FlushLogEntry flushLogEntry = builder.build();
    assertNotNull(flushLogEntry);
    FlushLogEntryProto protobuf = flushLogEntry.getProtobuf();
    assertEquals(dbSequenceNumber, protobuf.getDbSequenceNumber());
    assertEquals(flushTime, protobuf.getFlushTime());
    assertEquals(fileInfo, FlushFileInfo.getFromProtobuf(protobuf.getFileInfo()));

    if (flushReason != null) {
      assertEquals(flushReason, protobuf.getFlushReason());
    } else {
      assertFalse(protobuf.hasFlushReason());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushLogEntryValidScenarios")
  public void testFromProtobuf(
      String description,
      long dbSequenceNumber,
      long flushTime,
      FlushFileInfo fileInfo,
      String flushReason) {

    FlushLogEntryProto.Builder builder = FlushLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber)
        .setFlushTime(flushTime)
        .setFileInfo(fileInfo.getProtobuf());

    if (flushReason != null) {
      builder.setFlushReason(flushReason);
    }

    FlushLogEntryProto protobuf = builder.build();

    FlushLogEntry flushLogEntry = FlushLogEntry.getFromProtobuf(protobuf);

    assertNotNull(flushLogEntry);
    assertEquals(dbSequenceNumber, flushLogEntry.getDbSequenceNumber());
    assertEquals(flushTime, flushLogEntry.getFlushTime());
    assertEquals(fileInfo, flushLogEntry.getFileInfo());
    assertEquals(flushReason, flushLogEntry.getFlushReason());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushLogEntryValidScenarios")
  public void testToBuilder(
      String description,
      long dbSequenceNumber,
      long flushTime,
      FlushFileInfo fileInfo,
      String flushReason) {

    FlushLogEntry.Builder builder = new FlushLogEntry
        .Builder(dbSequenceNumber, flushTime, fileInfo);

    if (flushReason != null) {
      builder.setFlushReason(flushReason);
    }

    FlushLogEntry original = builder.build();
    FlushLogEntry rebuilt = original.toBuilder().build();

    assertNotNull(rebuilt);
    assertEquals(original.getDbSequenceNumber(), rebuilt.getDbSequenceNumber());
    assertEquals(original.getFlushTime(), rebuilt.getFlushTime());
    assertEquals(original.getFileInfo(), rebuilt.getFileInfo());
    assertEquals(original.getFlushReason(), rebuilt.getFlushReason());
  }
}
