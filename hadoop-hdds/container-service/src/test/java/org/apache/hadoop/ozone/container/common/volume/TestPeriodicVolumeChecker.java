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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.HEALTHY;
import static org.apache.hadoop.ozone.container.common.volume.TestStorageVolumeChecker.makeVolumes;
import static org.apache.hadoop.ozone.container.common.volume.TestVolumeSet.assertNumVolumes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.util.FakeTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test periodic volume checker in StorageVolumeChecker.
 */
public class TestPeriodicVolumeChecker {

  private static final Logger LOG = LoggerFactory.getLogger(TestPeriodicVolumeChecker.class);

  @TempDir
  private Path folder;

  private OzoneConfiguration conf = new OzoneConfiguration();
  private MutableVolumeSet volumeSet;
  private MutableVolumeSet metaVolumeSet;

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.toString());
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        Files.createDirectory(folder.resolve("VolumeCheckerDir")).toString());
    String scmId = UUID.randomUUID().toString();
    String datanodeUuid = UUID.randomUUID().toString();
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    metaVolumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.META_VOLUME, null);
  }

  @Test
  public void testPeriodicVolumeChecker(TestInfo testInfo) throws Exception {
    LOG.info("Executing {}", testInfo.getTestMethod());

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    Duration gap = dnConf.getDiskCheckMinGap();
    Duration interval = Duration.ofMinutes(dnConf.getPeriodicDiskCheckIntervalMinutes());

    FakeTimer timer = new FakeTimer();

    StorageVolumeChecker volumeChecker = new StorageVolumeChecker(conf, timer, "");
    BackgroundVolumeScannerMetrics metrics = volumeChecker.getMetrics();

    try {
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(2, HEALTHY)));
      volumeChecker.registerVolumeSet(new ImmutableVolumeSet(makeVolumes(1, HEALTHY)));
      volumeChecker.registerVolumeSet(volumeSet);
      volumeChecker.registerVolumeSet(metaVolumeSet);
      volumeChecker.setDelegateChecker(new TestStorageVolumeChecker.DummyChecker());

      assertEquals(0, metrics.getNumScanIterations());
      assertEquals(0, metrics.getNumDataVolumeScans());
      assertEquals(0, metrics.getNumMetadataVolumeScans());
      assertEquals(0, metrics.getNumVolumesScannedInLastIteration());
      assertNumVolumes(volumeSet, 1, 0);
      assertNumVolumes(metaVolumeSet, 1, 0);

      // first round
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      assertEquals(1, metrics.getNumScanIterations());
      assertEquals(1, metrics.getNumDataVolumeScans());
      assertEquals(1, metrics.getNumMetadataVolumeScans());
      assertEquals(5, metrics.getNumVolumesScannedInLastIteration());
      assertEquals(0, metrics.getNumIterationsSkipped());
      assertNumVolumes(volumeSet, 1, 0);
      assertNumVolumes(metaVolumeSet, 1, 0);

      // periodic disk checker next round within gap
      timer.advance(gap.toMillis() / 3);
      volumeChecker.checkAllVolumeSets();

      // skipped next round
      assertEquals(1, metrics.getNumScanIterations());
      assertEquals(1, metrics.getNumDataVolumeScans());
      assertEquals(1, metrics.getNumMetadataVolumeScans());
      assertEquals(5, metrics.getNumVolumesScannedInLastIteration());
      assertEquals(1, metrics.getNumIterationsSkipped());
      assertNumVolumes(volumeSet, 1, 0);
      assertNumVolumes(metaVolumeSet, 1, 0);

      // periodic disk checker next round
      timer.advance(interval.toMillis());
      volumeChecker.checkAllVolumeSets();

      assertEquals(2, metrics.getNumScanIterations());
      assertEquals(2, metrics.getNumDataVolumeScans());
      assertEquals(2, metrics.getNumMetadataVolumeScans());
      assertEquals(5, metrics.getNumVolumesScannedInLastIteration());
      assertEquals(1, metrics.getNumIterationsSkipped());
      assertNumVolumes(volumeSet, 1, 0);
      assertNumVolumes(metaVolumeSet, 1, 0);
    } finally {
      volumeChecker.shutdownAndWait(1, TimeUnit.SECONDS);
    }
  }
}
