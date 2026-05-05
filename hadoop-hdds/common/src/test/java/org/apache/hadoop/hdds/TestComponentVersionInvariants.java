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

package org.apache.hadoop.hdds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test to ensure Component version instances conform with invariants relied
 * upon in other parts of the codebase.
 */
public class TestComponentVersionInvariants {

  public static Stream<Arguments> values() {
    return Stream.of(
        arguments(
            DatanodeVersion.values(),
            DatanodeVersion.DEFAULT_VERSION,
            DatanodeVersion.FUTURE_VERSION),
        arguments(
            ClientVersion.values(),
            ClientVersion.DEFAULT_VERSION,
            ClientVersion.FUTURE_VERSION),
        arguments(
            OzoneManagerVersion.values(),
            OzoneManagerVersion.DEFAULT_VERSION,
            OzoneManagerVersion.FUTURE_VERSION)
    );
  }

  // FUTURE_VERSION is the latest
  @ParameterizedTest
  @MethodSource("values")
  public void testFutureVersionHasTheHighestOrdinal(
      ComponentVersion[] values, ComponentVersion defaultValue,
      ComponentVersion futureValue) {

    assertEquals(values[values.length - 1], futureValue);
  }

  // FUTURE_VERSION's internal version id is -1
  @ParameterizedTest
  @MethodSource("values")
  public void testFuturVersionHasMinusOneAsProtoRepresentation(
      ComponentVersion[] values, ComponentVersion defaultValue,
      ComponentVersion futureValue) {
    assertEquals(-1, futureValue.toProtoValue());

  }

  // DEFAULT_VERSION's internal version id is 0
  @ParameterizedTest
  @MethodSource("values")
  public void testDefaultVersionHasZeroAsProtoRepresentation(
      ComponentVersion[] values, ComponentVersion defaultValue,
      ComponentVersion futureValue) {
    assertEquals(0, defaultValue.toProtoValue());
  }

  // versions are increasing monotonically by one
  @ParameterizedTest
  @MethodSource("values")
  public void testAssignedProtoRepresentations(
      ComponentVersion[] values, ComponentVersion defaultValue,
      ComponentVersion futureValue) {
    int startValue = defaultValue.toProtoValue();
    // we skip the future version at the last position
    for (int i = 0; i < values.length - 1; i++) {
      assertEquals(values[i].toProtoValue(), startValue++);
    }
    assertEquals(values.length, ++startValue);
  }
}
