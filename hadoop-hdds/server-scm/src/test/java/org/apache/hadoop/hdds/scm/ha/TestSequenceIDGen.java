/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE;

public class TestSequenceIDGen {
  @Test
  public void testSequenceIDGenUponNonRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SequenceIdGen sequenceIdGen = new SequenceIdGen(
        conf, null, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 1000]
    Assert.assertEquals(1L, sequenceIdGen.getNextId());
    Assert.assertEquals(2L, sequenceIdGen.getNextId());
    Assert.assertEquals(3L, sequenceIdGen.getNextId());

    // default batchSize is 1000, the next batch is [1001, 2000]
    sequenceIdGen.invalidateBatch();
    Assert.assertEquals(1001, sequenceIdGen.getNextId());
    Assert.assertEquals(1002, sequenceIdGen.getNextId());
    Assert.assertEquals(1003, sequenceIdGen.getNextId());

    // default batchSize is 1000, the next batch is [2001, 3000]
    sequenceIdGen.invalidateBatch();
    Assert.assertEquals(2001, sequenceIdGen.getNextId());
    Assert.assertEquals(2002, sequenceIdGen.getNextId());
    Assert.assertEquals(2003, sequenceIdGen.getNextId());
  }

  @Test
  public void testSequenceIDGenUponRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    // enable ratis based SequenceIDGen
    conf.setBoolean(OZONE_SCM_HA_ENABLE_KEY, true);

    // change batchSize to 100
    conf.setInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE, 100);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SCMHAManager scmHAManager = MockSCMHAManager.getInstance(true);

    SequenceIdGen sequenceIdGen = new SequenceIdGen(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 100]
    Assert.assertEquals(1L, sequenceIdGen.getNextId());
    Assert.assertEquals(2L, sequenceIdGen.getNextId());
    Assert.assertEquals(3L, sequenceIdGen.getNextId());

    // the next batch is [101, 200]
    sequenceIdGen.invalidateBatch();
    Assert.assertEquals(101, sequenceIdGen.getNextId());
    Assert.assertEquals(102, sequenceIdGen.getNextId());
    Assert.assertEquals(103, sequenceIdGen.getNextId());

    // the next batch is [201, 300]
    sequenceIdGen.invalidateBatch();
    Assert.assertEquals(201, sequenceIdGen.getNextId());
    Assert.assertEquals(202, sequenceIdGen.getNextId());
    Assert.assertEquals(203, sequenceIdGen.getNextId());
  }
}
