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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test allocate container calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestAllocateContainer implements NonHATests.TestCase {

  private OzoneConfiguration conf;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeAll
  void init() throws Exception {
    conf = cluster().getConf();
    storageContainerLocationClient =
        cluster().getStorageContainerLocationClient();
  }

  @AfterAll
  void cleanup() {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testAllocate() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf),
            OzoneConsts.OZONE);
    assertNotNull(container);
    assertNotNull(container.getPipeline().getFirstNode());

  }

  @Test
  public void testAllocateNull() {
    assertThrows(NullPointerException.class, () ->
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), null));
  }

  @Test
  public void testAllocateRatis() throws Exception {
    testAllocateContainer(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
  }
  
  @Test
  public void testAllocateEC() throws Exception {
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    testAllocateContainer(ecReplicationConfig);
  }

  private void testAllocateContainer(ReplicationConfig replicationConfig) throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(replicationConfig, OzoneConsts.OZONE);

    assertNotNull(container);
    assertNotNull(container.getPipeline().getFirstNode());
  }
}
