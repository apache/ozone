/**
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
package org.apache.hadoop.hdds.scm.container.common.helpers;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Tests the exclude nodes list behavior at client.
 */
public class TestExcludeList {
  private TestClock clock = new TestClock(Instant.now(), ZoneOffset.UTC);

  @Test
  public void excludeNodesShouldBeCleanedBasedOnGivenTime() {
    ExcludeList list = new ExcludeList(10, clock);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.1").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    Assertions.assertTrue(list.getDatanodes().size() == 1);
    clock.fastForward(11);
    Assertions.assertTrue(list.getDatanodes().size() == 0);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.2").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.3").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    Assertions.assertTrue(list.getDatanodes().size() == 2);
  }

  @Test
  public void excludeNodeShouldNotBeCleanedIfExpiryTimeIsZero() {
    ExcludeList list = new ExcludeList(0, clock);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.1").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    Assertions.assertTrue(list.getDatanodes().size() == 1);
    clock.fastForward(1);
    Assertions.assertTrue(list.getDatanodes().size() == 1);
  }
}
