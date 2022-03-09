/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds;

import org.apache.hadoop.ozone.ClientVersion;
import org.assertj.core.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Test to ensure Component version instances conform with invariants relied
 * upon in other parts of the codebase.
 */
@RunWith(Parameterized.class)
public class TestComponentVersionInvariants {

  @Parameterized.Parameters
  public static Object[][] values() {
    Object[][] values = new Object[2][];
    values[0] =
        Arrays.array(
            DatanodeVersion.values(),
            DatanodeVersion.DEFAULT_VERSION,
            DatanodeVersion.FUTURE_VERSION);
    values[1] =
        Arrays.array(
            ClientVersion.values(),
            ClientVersion.DEFAULT_VERSION,
            ClientVersion.FUTURE_VERSION);
    return values;
  }

  private final ComponentVersion[] values;
  private final ComponentVersion defaultValue;
  private final ComponentVersion futureValue;

  public TestComponentVersionInvariants(ComponentVersion[] values,
      ComponentVersion defaultValue, ComponentVersion futureValue) {
    this.values = new ComponentVersion[values.length];
    System.arraycopy(values, 0, this.values, 0, values.length);
    this.defaultValue = defaultValue;
    this.futureValue = futureValue;
  }

  // FUTURE_VERSION is the latest
  @Test
  public void testFutureVersionHasTheHighestOrdinal() {
    assertEquals(values[values.length - 1], futureValue);
  }

  // FUTURE_VERSION's internal version id is -1
  @Test
  public void testFuturVersionHasMinusOneAsProtoRepresentation() {
    assertEquals(-1, futureValue.toProtoValue());

  }

  // DEFAULT_VERSION's internal version id is 0
  @Test
  public void testDefaultVersionHasZeroAsProtoRepresentation() {
    assertEquals(0, defaultValue.toProtoValue());
  }

  // versions are increasing monotonically by one
  @Test
  public void testAssignedProtoRepresentations() {
    int startValue = defaultValue.toProtoValue();
    // we skip the future version at the last position
    for (int i = 0; i < values.length - 1; i++) {
      assertEquals(values[i].toProtoValue(), startValue++);
    }
    assertEquals(values.length, ++startValue);
  }
}
