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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.util.FakeTimer;
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
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.HEALTHY;
import static org.apache.hadoop.ozone.container.common.volume.TestStorageVolumeChecker.makeVolumes;

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
    Duration gap = dnConf.getDiskCheckMinGap();
    Duration interval = Duration.ofMinutes(
        dnConf.getPeriodicDiskCheckIntervalMinutes());

    FakeTimer timer = new FakeTimer();

    StorageVolumeChecker volumeChecker = new StorageVolumeChecker(conf, timer);

    try {
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(
          2, HEALTHY)));
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(
          1, HEALTHY)));
      volumeChecker.setDelegateChecker(
          new TestStorageVolumeChecker.DummyChecker());

      Assert.assertEquals(0, volumeChecker.getNumAllVolumeChecks());
      Assert.assertEquals(0, volumeChecker.getNumAllVolumeSetsChecks());

      // first round
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      Assert.assertEquals(2, volumeChecker.getNumAllVolumeChecks());
      Assert.assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
      Assert.assertEquals(0, volumeChecker.getNumSkippedChecks());

      // periodic disk checker next round within gap
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      // skipped next round
      Assert.assertEquals(2, volumeChecker.getNumAllVolumeChecks());
      Assert.assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
      Assert.assertEquals(1, volumeChecker.getNumSkippedChecks());

      // periodic disk checker next round
      timer.advance(interval.toMillis());
      volumeChecker.checkAllVolumeSets();

      Assert.assertEquals(4, volumeChecker.getNumAllVolumeChecks());
      Assert.assertEquals(2, volumeChecker.getNumAllVolumeSetsChecks());
      Assert.assertEquals(1, volumeChecker.getNumSkippedChecks());
    } finally {
      volumeChecker.shutdownAndWait(1, TimeUnit.SECONDS);
    }
  }
}
