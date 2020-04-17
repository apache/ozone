/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Test class for SCMStateMachine.
 */
public class TestSCMStateMachine {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private SCMStateMachine scmStateMachine;
  private StorageContainerManager scm;
  private SCMRatisServer scmRatisServer;
  private OzoneConfiguration conf;
  private String scmId;
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.set(ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID, "scm-ha-test");
    scmId = UUID.randomUUID().toString();

    initSCM();
    scm = HddsTestUtils.getScm(conf);
    scm.start();
    scmRatisServer = scm.getScmRatisServer();
    scmStateMachine = scm.getScmRatisServer().getScmStateMachine();
  }

  @Test
  public void testSCMUpdatedAppliedIndex(){
    // State machine should start with 0 term and 0 index.
    scmStateMachine.notifyIndexUpdate(0, 0);
    Assert.assertEquals(0,
        scmStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(0,
        scmStateMachine.getLastAppliedTermIndex().getIndex());

    // If only the transactionMap is updated, index should stay 0.
    scmStateMachine.addApplyTransactionTermIndex(0, 1);
    Assert.assertEquals(0L,
        scmStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(0L,
        scmStateMachine.getLastAppliedTermIndex().getIndex());

    // After the index update is notified, the index should increase.
    scmStateMachine.notifyIndexUpdate(0, 1);
    Assert.assertEquals(0L,
        scmStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(1L,
        scmStateMachine.getLastAppliedTermIndex().getIndex());

    // Only do a notifyIndexUpdate can also increase the index.
    scmStateMachine.notifyIndexUpdate(0, 2);
    Assert.assertEquals(0L,
        scmStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(2L,
        scmStateMachine.getLastAppliedTermIndex().getIndex());

    // If a larger index is notified, the index should not be updated.
    scmStateMachine.notifyIndexUpdate(0, 5);
    Assert.assertEquals(0L,
        scmStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(2L,
        scmStateMachine.getLastAppliedTermIndex().getIndex());
  }

  private void initSCM() throws IOException {
    String clusterId = UUID.randomUUID().toString();
    final String path = folder.newFolder().toString();
    Path scmPath = Paths.get(path, "scm-meta");
    Files.createDirectories(scmPath);
    conf.set(OZONE_METADATA_DIRS, scmPath.toString());
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
  }

  @After
  public void cleanup() {
    scm.stop();
  }
}
