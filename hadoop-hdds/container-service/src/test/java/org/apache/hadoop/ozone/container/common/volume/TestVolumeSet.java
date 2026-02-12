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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.volume.HddsVolume.HDDS_VOLUME_DIR;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link MutableVolumeSet} operations.
 */
public class TestVolumeSet {

  private OzoneConfiguration conf;
  private MutableVolumeSet volumeSet;

  @TempDir
  private Path baseDir;

  private String volume1;
  private String volume2;
  private final List<String> volumes = new ArrayList<>();

  private void initializeVolumeSet() throws Exception {
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
  }

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    volume1 = baseDir.resolve("disk1").toString();
    volume2 = baseDir.resolve("disk2").toString();

    String dataDirKey = volume1 + "," + volume2;
    volumes.add(volume1);
    volumes.add(volume2);
    conf.set(HDDS_DATANODE_DIR_KEY, dataDirKey);
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        dataDirKey);
    initializeVolumeSet();
  }

  @AfterEach
  public void shutdown() throws IOException {
    // Delete the volume root dir
    List<StorageVolume> vols = new ArrayList<>();
    vols.addAll(volumeSet.getVolumesList());
    vols.addAll(volumeSet.getFailedVolumesList());

    for (StorageVolume volume : vols) {
      FileUtils.deleteDirectory(volume.getStorageDir());
    }
    volumeSet.shutdown();
  }

  private boolean checkVolumeExistsInVolumeSet(String volumeRoot) {
    for (StorageVolume volume : volumeSet.getVolumesList()) {
      if (volume.getStorageDir().getPath().equals(volumeRoot)
          || volume.getStorageDir().getParent().equals(volumeRoot)) {
        return true;
      }
    }
    return false;
  }

  static void assertNumVolumes(MutableVolumeSet volumeSet, int expectedHealthyVolumes, int expectedFailedVolumes) {
    MetricsRecordBuilder metricsRecords = getMetrics(volumeSet.getVolumeHealthMetrics());
    assertGauge("TotalVolumes", expectedHealthyVolumes + expectedFailedVolumes, metricsRecords);
    assertGauge("NumHealthyVolumes", expectedHealthyVolumes, metricsRecords);
    assertGauge("NumFailedVolumes", expectedFailedVolumes, metricsRecords);
  }

  @Test
  public void testVolumeSetInitialization() throws Exception {

    List<StorageVolume> volumesList = volumeSet.getVolumesList();

    // VolumeSet initialization should add volume1 and volume2 to VolumeSet
    assertEquals(volumesList.size(), volumes.size(),
        "VolumeSet initialization is incorrect");
    assertTrue(checkVolumeExistsInVolumeSet(volume1),
        "VolumeSet not initialized correctly");
    assertTrue(checkVolumeExistsInVolumeSet(volume2),
        "VolumeSet not initialized correctly");

    assertNumVolumes(volumeSet, 2, 0);
  }

  @Test
  public void testFailVolume() throws Exception {
    assertNumVolumes(volumeSet, 2, 0);

    //Fail a volume
    volumeSet.failVolume(HddsVolumeUtil.getHddsRoot(volume1));

    // Failed volume should not show up in the volumeList
    assertEquals(1, volumeSet.getVolumesList().size());

    // Failed volume should be added to FailedVolumeList
    assertEquals(1, volumeSet.getFailedVolumesList().size(),
        "Failed volume not present in FailedVolumeMap");
    assertEquals(HddsVolumeUtil.getHddsRoot(volume1),
        volumeSet.getFailedVolumesList().get(0).getStorageDir().getPath(),
        "Failed Volume list did not match");

    // Failed volume should not exist in VolumeMap
    assertThat(volumeSet.getVolumeMap()).doesNotContainKey(volume1);

    assertNumVolumes(volumeSet, 1, 1);
  }

  @Test
  public void testVolumeInInconsistentState() throws Exception {
    assertNumVolumes(volumeSet, 2, 0);
    assertEquals(2, volumeSet.getVolumesList().size());

    // Add a volume to VolumeSet
    String volume3 = baseDir + "disk3";

    // Create the root volume dir and create a sub-directory within it.
    File newVolume = new File(volume3, HDDS_VOLUME_DIR);
    System.out.println("new volume root: " + newVolume);
    assertTrue(newVolume.mkdirs());
    assertTrue(newVolume.exists(), "Failed to create new volume root");
    File dataDir = new File(newVolume, "chunks");
    assertTrue(dataDir.mkdirs());
    assertTrue(dataDir.exists());

    // The new volume is in an inconsistent state as the root dir is
    // non-empty but the version file does not exist. Add Volume should
    // return false.
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, conf.get(ScmConfigKeys.HDDS_DATANODE_DIR_KEY) + "," + volume3);
    volumeSet.shutdown();
    initializeVolumeSet();

    assertEquals(2, volumeSet.getVolumesList().size());
    assertFalse(checkVolumeExistsInVolumeSet(volume3), "AddVolume should fail" +
        " for an inconsistent volume");

    assertNumVolumes(volumeSet, 2, 1);
    // Delete volume3
    File volume = new File(volume3);
    FileUtils.deleteDirectory(volume);
  }

  @Test
  public void testShutdown() throws Exception {
    assertNumVolumes(volumeSet, 2, 0);
    List<StorageVolume> volumesList = volumeSet.getVolumesList();

    volumeSet.shutdown();

    // Verify that volume usage can be queried during shutdown.
    for (StorageVolume volume : volumesList) {
      assertThat(volume.getVolumeUsage()).isPresent();
      volume.getCurrentUsage();
    }
  }

  @Test
  void testFailVolumes(@TempDir File readOnlyVolumePath, @TempDir File volumePath) throws Exception {
    //Set to readonly, so that this volume will be failed
    assumeThat(readOnlyVolumePath.setReadOnly()).isTrue();
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(HDDS_DATANODE_DIR_KEY, readOnlyVolumePath.getAbsolutePath()
        + "," + volumePath.getAbsolutePath());
    ozoneConfig.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        volumePath.getAbsolutePath());
    MutableVolumeSet volSet = new MutableVolumeSet(UUID.randomUUID().toString(), ozoneConfig,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    assertEquals(1, volSet.getFailedVolumesList().size());
    assertEquals(readOnlyVolumePath, volSet.getFailedVolumesList().get(0)
        .getStorageDir());
    assertNumVolumes(volSet, 1, 1);
    volSet.shutdown();
  }

  @Test
  public void testInterrupt() throws Exception {
    Method method = this.volumeSet.getClass()
        .getDeclaredMethod("checkAllVolumes");
    method.setAccessible(true);
    String exceptionMessage = "";

    try {
      Thread.currentThread().interrupt();
      method.invoke(this.volumeSet);
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Interruption Occur");
      }
    } catch (InterruptedException interruptedException) {
      exceptionMessage = "InterruptedException Occur.";
    }

    assertEquals(
        "InterruptedException Occur.",
        exceptionMessage);
  }

}
