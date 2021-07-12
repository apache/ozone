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

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * Test periodic volume checker in StorageVolumeChecker.
 */
public class TestPeriodicVolumeChecker {

  public static final Logger LOG = LoggerFactory.getLogger(
      TestPeriodicVolumeChecker.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public Timeout globalTimeout = Timeout.seconds(150);

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.getRoot()
        .getAbsolutePath());
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        folder.newFolder().getAbsolutePath());
  }

  @After
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(folder.getRoot());
  }

  @Test
  public void testPeriodicVolumeChecker() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    dnConf.setDiskCheckMinGap(Duration.ofMinutes(2));
    dnConf.setPeriodicDiskCheckIntervalMinutes(1);
    conf.setFromObject(dnConf);

    DatanodeDetails datanodeDetails =
        ContainerTestUtils.createDatanodeDetails();
    OzoneContainer ozoneContainer =
        ContainerTestUtils.getOzoneContainer(datanodeDetails, conf);
    MutableVolumeSet dataVolumeSet = ozoneContainer.getVolumeSet();

    StorageVolumeChecker volumeChecker = dataVolumeSet.getVolumeChecker();
    volumeChecker.setDelegateChecker(
        new TestStorageVolumeChecker.DummyChecker());

    // 1 for volumeSet and 1 for metadataVolumeSet
    // in MutableVolumeSet constructor
    Assert.assertEquals(2, volumeChecker.getNumAllVolumeChecks());
    Assert.assertEquals(0, volumeChecker.getNumAllVolumeSetsChecks());

    // wait for periodic disk checker start
    Thread.sleep((60 + 5) * 1000);

    // first round
    // 2 for volumeSet and 2 for metadataVolumeSet
    Assert.assertEquals(4, volumeChecker.getNumAllVolumeChecks());
    Assert.assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
    Assert.assertEquals(0, volumeChecker.getNumSkippedChecks());

    // wait for periodic disk checker next round
    Thread.sleep((60 + 5) * 1000);

    // skipped next round
    Assert.assertEquals(4, volumeChecker.getNumAllVolumeChecks());
    Assert.assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
    Assert.assertEquals(1, volumeChecker.getNumSkippedChecks());
  }
}
