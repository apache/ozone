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
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests the exclude nodes list behavior at client.
 */
public class TestExcludeList {

  @Test
  public void autoCleanerShouldCleanTheNodesWhichArePresentForLong()
      throws InterruptedException {
    ExcludeList list = new ExcludeList();
    list.startAutoExcludeNodesCleaner(10, 5);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.1").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    Assert.assertTrue(list.getDatanodes().size() == 1);
    Thread.sleep(20);
    Assert.assertTrue(list.getDatanodes().size() == 0);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.2").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.3").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    Assert.assertTrue(list.getDatanodes().size() == 2);
  }

  @Test
  public void stopAutoCleanerShouldNotCleanTheNodes()
      throws InterruptedException {
    ExcludeList list = new ExcludeList();
    list.startAutoExcludeNodesCleaner(10, 5);
    list.addDatanode(DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .setIpAddress("127.0.0.1").setHostName("localhost").addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 2001))
        .build());
    list.stopAutoExcludeNodesCleaner();
    Assert.assertTrue(list.getDatanodes().size() == 1);
    Thread.sleep(20);
    Assert.assertTrue(list.getDatanodes().size() == 1);
  }
}
