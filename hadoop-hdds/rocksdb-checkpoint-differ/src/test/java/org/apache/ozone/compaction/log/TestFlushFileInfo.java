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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.FlushFileInfoProto;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for FlushFileInfo.
 */
public class TestFlushFileInfo {

  private static Stream<Arguments> flushFileInfoValidScenarios() {
    return Stream.of(
        Arguments.of("All parameters are present.",
            "fileName",
            "startRange",
            "endRange",
            "columnFamily"
        ),
        Arguments.of("Only fileName is present.",
            "fileName",
            null,
            null,
            null
        ),
        Arguments.of("Only fileName is present (duplicate).",
            "fileName",
            null,
            null,
            null
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushFileInfoValidScenarios")
  public void testFlushFileInfoValidScenario(String description,
                                             String fileName,
                                             String startRange,
                                             String endRange,
                                             String columnFamily) {

    FlushFileInfo.Builder builder = new FlushFileInfo.Builder(fileName)
        .setStartRange(startRange)
        .setEndRange(endRange)
        .setColumnFamily(columnFamily);
    FlushFileInfo flushFileInfo = builder.build();
    assertNotNull(flushFileInfo);
    FlushFileInfo prunedFlushFileInfo = builder.setPruned().build();
    assertFalse(flushFileInfo.isPruned());
    flushFileInfo.setPruned();
    assertTrue(flushFileInfo.isPruned());
    assertTrue(prunedFlushFileInfo.isPruned());
  }

  private static Stream<Arguments> flushFileInfoInvalidScenarios() {
    return Stream.of(
        Arguments.of("All parameters are null.",
            null,
            null,
            null,
            null,
            "FileName is required parameter."
        ),
        Arguments.of("fileName is null.",
            null,
            "startRange",
            "endRange",
            "columnFamily",
            "FileName is required parameter."
        ),
        Arguments.of("startRange is not present.",
            "fileName",
            null,
            "endRange",
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'endRange', columnFamily: 'columnFamily'."
        ),
        Arguments.of("endRange is not present.",
            "fileName",
            "startRange",
            null,
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'startRange', " +
                "endRange: 'null', columnFamily: 'columnFamily'."
        ),
        Arguments.of("columnFamily is not present.",
            "fileName",
            "startRange",
            "endRange",
            null,
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'startRange', " +
                "endRange: 'endRange', columnFamily: 'null'."
        ),
        Arguments.of("startRange and endRange are not present.",
            "fileName",
            null,
            null,
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'null', columnFamily: 'columnFamily'."
        ),
        Arguments.of("endRange and columnFamily are not present.",
            "fileName",
            "startRange",
            null,
            null,
            "Either all of startRange, endRange and columnFamily " +
                "should be non-null or null. startRange: 'startRange', " +
                "endRange: 'null', columnFamily: 'null'."
        ),
        Arguments.of("startRange and columnFamily are not present.",
            "fileName",
            null,
            "endRange",
            null,
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'endRange', columnFamily: 'null'."
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushFileInfoInvalidScenarios")
  public void testFlushFileInfoInvalidScenario(String description,
                                               String fileName,
                                               String startRange,
                                               String endRange,
                                               String columnFamily,
                                               String expectedMessage) {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> new FlushFileInfo.Builder(fileName)
            .setStartRange(startRange)
            .setEndRange(endRange)
            .setColumnFamily(columnFamily)
            .build());
    assertEquals(expectedMessage, exception.getMessage());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushFileInfoValidScenarios")
  public void testGetProtobuf(String description,
                              String fileName,
                              String startRange,
                              String endRange,
                              String columnFamily) {
    FlushFileInfo flushFileInfo = new FlushFileInfo
        .Builder(fileName)
        .setStartRange(startRange)
        .setEndRange(endRange)
        .setColumnFamily(columnFamily)
        .build();

    FlushFileInfoProto protobuf = flushFileInfo.getProtobuf();
    assertEquals(fileName, protobuf.getFileName());

    if (startRange != null) {
      assertEquals(startRange, protobuf.getStartKey());
    } else {
      assertFalse(protobuf.hasStartKey());
    }
    if (endRange != null) {
      assertEquals(endRange, protobuf.getEndKey());
    } else {
      assertFalse(protobuf.hasEndKey());
    }
    if (columnFamily != null) {
      assertEquals(columnFamily, protobuf.getColumnFamily());
    } else {
      assertFalse(protobuf.hasColumnFamily());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("flushFileInfoValidScenarios")
  public void testFromProtobuf(String description,
                               String fileName,
                               String startRange,
                               String endRange,
                               String columnFamily) {
    FlushFileInfoProto.Builder builder = FlushFileInfoProto
        .newBuilder()
        .setFileName(fileName);

    if (startRange != null) {
      builder = builder.setStartKey(startRange);
    }
    if (endRange != null) {
      builder = builder.setEndKey(endRange);
    }
    if (columnFamily != null) {
      builder = builder.setColumnFamily(columnFamily);
    }

    FlushFileInfoProto protobuf = builder.build();

    FlushFileInfo flushFileInfo =
        FlushFileInfo.getFromProtobuf(protobuf);

    assertEquals(fileName, flushFileInfo.getFileName());
    assertEquals(startRange, flushFileInfo.getStartKey());
    assertEquals(endRange, flushFileInfo.getEndKey());
    assertEquals(columnFamily, flushFileInfo.getColumnFamily());
    assertFalse(flushFileInfo.isPruned());

    FlushFileInfoProto unPrunedProtobuf = builder.setPruned(false).build();
    FlushFileInfo unPrunedFlushFileInfo =
        FlushFileInfo.getFromProtobuf(unPrunedProtobuf);
    assertFalse(unPrunedFlushFileInfo.isPruned());

    FlushFileInfoProto prunedProtobuf = builder.setPruned(true).build();
    FlushFileInfo prunedFlushFileInfo =
        FlushFileInfo.getFromProtobuf(prunedProtobuf);
    assertTrue(prunedFlushFileInfo.isPruned());
  }
}
