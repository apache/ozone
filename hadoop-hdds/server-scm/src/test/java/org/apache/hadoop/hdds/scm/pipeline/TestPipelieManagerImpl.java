/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * Tests for PipelineManagerImpl.
 */
public class TestPipelieManagerImpl {
  private PipelineManagerV2Impl pipelineManager;
  private File testDir;
  private DBStore dbStore;

  @Before
  public void init() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, new SCMDBDefinition());
    pipelineManager = PipelineManagerV2Impl.newPipelineManager(
        conf, MockSCMHAManager.getInstance(),
        new MockNodeManager(true, 20),
        SCMDBDefinition.PIPELINES.getTable(dbStore), new EventQueue());
  }

  @After
  public void cleanup() throws Exception {
    if (pipelineManager != null) {
      pipelineManager.close();
    }
    if (dbStore != null) {
      dbStore.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testCreatePipeline() throws Exception {
    Assert.assertTrue(pipelineManager.getPipelines().isEmpty());
    pipelineManager.allowPipelineCreation();
    Pipeline pipeline1 = pipelineManager.createPipeline(
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.THREE);
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
    Assert.assertTrue(pipelineManager.containsPipeline(pipeline1.getId()));

    Pipeline pipeline2 = pipelineManager.createPipeline(
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE);
    Assert.assertEquals(2, pipelineManager.getPipelines().size());
    Assert.assertTrue(pipelineManager.containsPipeline(pipeline2.getId()));
  }
}
