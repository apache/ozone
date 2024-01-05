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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.util.FakeTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.HEALTHY;
import static org.apache.hadoop.ozone.container.common.volume.TestStorageVolumeChecker.makeVolumes;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test periodic volume checker in StorageVolumeChecker.
 */
@Timeout(150)
public class TestPeriodicVolumeChecker {

  public static final Logger LOG = LoggerFactory.getLogger(
      TestPeriodicVolumeChecker.class);

  @TempDir
  private Path folder;

  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.toString());
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        Files.createDirectory(folder.resolve("VolumeCheckerDir")).toString());
  }

  @Test
  public void testPeriodicVolumeChecker(TestInfo testInfo) throws Exception {
    LOG.info("Executing {}", testInfo.getTestMethod());

    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    Duration gap = dnConf.getDiskCheckMinGap();
    Duration interval = Duration.ofMinutes(
        dnConf.getPeriodicDiskCheckIntervalMinutes());

    FakeTimer timer = new FakeTimer();

    StorageVolumeChecker volumeChecker = new StorageVolumeChecker(conf, timer,
        "");

    try {
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(
          2, HEALTHY)));
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(
          1, HEALTHY)));
      volumeChecker.setDelegateChecker(
          new TestStorageVolumeChecker.DummyChecker());

      assertEquals(0, volumeChecker.getNumAllVolumeChecks());
      assertEquals(0, volumeChecker.getNumAllVolumeSetsChecks());

      // first round
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      assertEquals(2, volumeChecker.getNumAllVolumeChecks());
      assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
      assertEquals(0, volumeChecker.getNumSkippedChecks());

      // periodic disk checker next round within gap
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      // skipped next round
      assertEquals(2, volumeChecker.getNumAllVolumeChecks());
      assertEquals(1, volumeChecker.getNumAllVolumeSetsChecks());
      assertEquals(1, volumeChecker.getNumSkippedChecks());

      // periodic disk checker next round
      timer.advance(interval.toMillis());
      volumeChecker.checkAllVolumeSets();

      assertEquals(4, volumeChecker.getNumAllVolumeChecks());
      assertEquals(2, volumeChecker.getNumAllVolumeSetsChecks());
      assertEquals(1, volumeChecker.getNumSkippedChecks());
    } finally {
      volumeChecker.shutdownAndWait(1, TimeUnit.SECONDS);
    }
  }
}
