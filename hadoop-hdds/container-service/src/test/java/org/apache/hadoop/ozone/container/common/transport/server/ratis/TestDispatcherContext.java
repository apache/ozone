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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.HDDSVersion;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link DispatcherContext} write version handling (HDDS-15718).
 */
public class TestDispatcherContext {

  @Test
  void writeVersionDefaultsToZdu() {
    DispatcherContext context = DispatcherContext
        .newBuilder(DispatcherContext.Op.WRITE_STATE_MACHINE_DATA)
        .build();

    assertEquals(HDDSVersion.ZDU,
        context.getWriteVersion());
  }

  @Test
  void writeVersionIsCarried() {
    // Use a non-default version so this exercises the setter, not the ZDU default.
    DispatcherContext context = DispatcherContext
        .newBuilder(DispatcherContext.Op.WRITE_STATE_MACHINE_DATA)
        .setWriteVersion(HDDSVersion.SHORT_CIRCUIT_READS)
        .build();

    assertEquals(HDDSVersion.SHORT_CIRCUIT_READS,
        context.getWriteVersion());
  }
}
