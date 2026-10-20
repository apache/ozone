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

package org.apache.hadoop.ozone.jobworker.volume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link VolatileJobworkerVolumeSet} operations.
 */
public class TestJobworkerVolumeSet {

  private static final int VOLUME_COUNT = 2;
  private final String jobworkerUuid = UUID.randomUUID().toString();
  private final String clusterId = UUID.randomUUID().toString();
  private OzoneConfiguration conf;
  private VolatileJobworkerVolumeSet volumeSet;
  @TempDir
  private Path tempDir;
  private String volume1;
  private String volume2;
  private List<String> volumes;
  private JobworkerStateContext mockContext;

  @BeforeEach
  public void setup() throws Exception {
    // Create mock JobworkerStateContext
    mockContext = Mockito.mock(JobworkerStateContext.class);

    // Setup configuration with volume paths
    conf = new OzoneConfiguration();
    JobworkerConfiguration jwConf = conf.getObject(JobworkerConfiguration.class);
    volume1 = new File(tempDir.toFile(), "disk1").getAbsolutePath();
    volume2 = new File(tempDir.toFile(), "disk2").getAbsolutePath();
    volumes = Arrays.asList(volume1, volume2);

    String dataDirKey = String.join(",", volumes);
    jwConf.setStorageVolumeDirs(dataDirKey);
    conf.setFromObject(jwConf);
    volumeSet = new VolatileJobworkerVolumeSet(jobworkerUuid, conf, mockContext);
  }

  @AfterEach
  public void close() throws IOException {
    if (volumeSet != null) {
      volumeSet.close();
    }

    // Clean up test directory
    for (String volume : volumes) {
      FileUtils.deleteDirectory(new File(volume));
    }
  }

  @Test
  public void testVolumeSetInitialization() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);

    List<VolatileJobworkerVolume> volumesList = volumeSet.getVolumesList();
    assertEquals(VOLUME_COUNT, volumesList.size(), "Volume list should contain both volumes");

    // Volume paths should match the configured paths
    boolean foundVolume1 = false;
    boolean foundVolume2 = false;
    for (VolatileJobworkerVolume volume : volumesList) {
      String volumePath = volume.getJobworkerDir().getPath();
      if (volumePath.startsWith(volume1)) {
        foundVolume1 = true;
      } else if (volumePath.startsWith(volume2)) {
        foundVolume2 = true;
      }
    }

    assertTrue(foundVolume1, "Volume 1 should be in the volume set");
    assertTrue(foundVolume2, "Volume 2 should be in the volume set");

    Set<String> storageIds = new HashSet<>();
    for (VolatileJobworkerVolume volume : volumesList) {
      assertEquals(clusterId, volume.getClusterID(), "ClusterID should match");
      assertEquals(jobworkerUuid, volume.getJobworkerUuid(), "JobworkerUUID should match");
      assertEquals(VolatileJobworkerVolume.VolumeState.NORMAL, volume.getState(), "Volume should be in NORMAL state");
      storageIds.add(volume.getStorageID());
    }
    // Each volume has a unique storageId
    assertEquals(VOLUME_COUNT, storageIds.size());
  }

  @Test
  public void testVolumeSetInitializationFailedVolume() throws Exception {
    boolean unWritable = tempDir.toFile().setWritable(false);
    String taskId = "test-task-" + UUID.randomUUID();
    try {
      volumeSet.initializeVolumeSet(clusterId);

      for (VolatileJobworkerVolume jobworkerVolume : volumeSet.getVolumesList()) {
        assertEquals(VolatileJobworkerVolume.VolumeState.FAILED, jobworkerVolume.getState());
      }
      assertNull(volumeSet.chooseVolume(),
          "All the volumes are failure, so chooseVolume should return a valid volume");
      Exception exception = assertThrows(IOException.class, () -> {
        volumeSet.createTaskDirectory(taskId);
      });
      assertTrue(exception.getMessage().contains("No viable volumes available"));

      boolean cleanResult = volumeSet.cleanupTask(taskId);
      assertFalse(cleanResult);
    } finally {
      if (unWritable) {
        tempDir.toFile().setWritable(true);
      }
    }
  }

  @Test
  public void testChooseVolume() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);

    VolatileJobworkerVolume selectedVolume = volumeSet.chooseVolume();
    assertNotNull(selectedVolume, "chooseVolume should return a valid volume");
    assertEquals(VolatileJobworkerVolume.VolumeState.NORMAL, selectedVolume.getState(),
        "Selected volume should be in NORMAL state");
  }

  @Test
  public void testCreateAndCleanupTaskDirectory() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);
    String taskId = "test-task-" + UUID.randomUUID();
    File taskDir = volumeSet.createTaskDirectory(taskId);

    assertNotNull(taskDir, "Task directory should be created");
    assertTrue(taskDir.exists(), "Task directory should exist on disk");
    assertTrue(taskDir.isDirectory(), "Task directory should be a directory");

    // Create some files in the task directory
    File dataFile = new File(taskDir, "data.txt");
    Files.write(dataFile.toPath(), "test data".getBytes());
    assertTrue(dataFile.exists(), "Data file should exist");

    boolean cleanResult = volumeSet.cleanupTask(taskId);
    assertTrue(cleanResult, "Task cleanup should succeed");
    assertFalse(taskDir.exists(), "Task directory should be removed after cleanup");
  }

  @Test
  public void testCreateMultipleTaskDirectory() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);
    String taskId1 = "test-task-" + UUID.randomUUID();
    String taskId2 = "test-task-" + UUID.randomUUID();
    File taskDir1 = volumeSet.createTaskDirectory(taskId1);
    File taskDir2 = volumeSet.createTaskDirectory(taskId2);

    // Clean up the task1
    assertTrue(volumeSet.cleanupTask(taskId1), "Task1 cleanup should succeed");
    assertFalse(taskDir1.exists(), "Task1 directory should be removed after cleanup");
    // Task2 task directory should not be affected
    assertTrue(taskDir2.exists(), "Task2 directory should exist");
    assertTrue(volumeSet.cleanupTask(taskId2), "Task2 cleanup should succeed");
    assertFalse(taskDir2.exists(), "Task1 directory should be removed after cleanup");
  }

  @Test
  public void testRemoveVolume() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);
    // Get initial volume count
    int initialVolumeCount = volumeSet.getVolumesList().size();
    assertEquals(2, initialVolumeCount, "Should have 2 volumes initially");
    String volumePath = volumeSet.getVolumesList().get(0).getJobworkerDir().getPath();

    volumeSet.removeVolume(volumePath);

    assertEquals(1, volumeSet.getVolumesList().size(),
        "Volume count should decrease after removal");

    // Verify the removed volume is actually gone
    boolean volumeExists = false;
    for (VolatileJobworkerVolume volume : volumeSet.getVolumesList()) {
      if (volume.getJobworkerDir().getPath().equals(volumePath)) {
        volumeExists = true;
        break;
      }
    }
    assertFalse(volumeExists, "Removed volume should not be in the volume list");
  }

  @Test
  public void testClose() throws Exception {
    volumeSet.initializeVolumeSet(clusterId);
    // Create multiple task directories
    String taskId1 = "test-task-" + UUID.randomUUID();
    String taskId2 = "test-task-" + UUID.randomUUID();
    File taskDir1 = volumeSet.createTaskDirectory(taskId1);
    File taskDir2 = volumeSet.createTaskDirectory(taskId2);
    assertTrue(taskDir1.exists(), "Task directory 1 should exist");
    assertTrue(taskDir2.exists(), "Task directory 2 should exist");

    List<VolatileJobworkerVolume> volumesList = volumeSet.getVolumesList();
    volumeSet.close();

    // Verify that volumes are cleared
    assertEquals(0, volumeSet.getVolumesList().size(),
        "Volume list should be empty after shutdown");

    // Verify that all directories are removed
    assertFalse(taskDir1.exists(), "Task directory 1 should be removed after shutdown");
    assertFalse(taskDir2.exists(), "Task directory 2 should be removed after shutdown");

    // Check that the jobworker directories are also removed
    for (VolatileJobworkerVolume volume : volumesList) {
      assertFalse(volume.getJobworkerDir().exists(),
          "Jobworker directory should be removed after shutdown");
    }
  }

  @Test
  public void testInitializeVolumeSetCalledMultipleTimes() throws Exception {
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(VolatileJobworkerVolumeSet.class));

    volumeSet.initializeVolumeSet(clusterId);
    int initialVolumeCount = volumeSet.getVolumesList().size();
    assertFalse(logCapturer.getOutput().contains("Ignore"));

    volumeSet.initializeVolumeSet(clusterId);
    // Call initialize again would normally be ignored
    assertTrue(logCapturer.getOutput().contains("Ignore"));
    assertEquals(initialVolumeCount, volumeSet.getVolumesList().size());
  }

  @Test
  public void testVolumeInitializationWithoutVolume() throws Exception {
    OzoneConfiguration badConf = new OzoneConfiguration();
    JobworkerConfiguration jwConf = badConf.getObject(JobworkerConfiguration.class);
    jwConf.setStorageVolumeDirs("");
    conf.setFromObject(jwConf);

    VolatileJobworkerVolumeSet emptySet = new VolatileJobworkerVolumeSet(jobworkerUuid, badConf, mockContext);

    emptySet.initializeVolumeSet(clusterId);
    assertEquals(0, emptySet.getVolumesList().size());
    emptySet.close();
  }

  @Test
  public void testCreateTaskDirectoryWithNoVolumes() throws Exception {
    OzoneConfiguration emptyVolumeConf = new OzoneConfiguration();
    // Initialize an empty volume set (no volumes)
    JobworkerConfiguration jwConf = emptyVolumeConf.getObject(JobworkerConfiguration.class);
    jwConf.setStorageVolumeDirs("");
    emptyVolumeConf.setFromObject(jwConf);

    VolatileJobworkerVolumeSet
        emptyVolumeSet = new VolatileJobworkerVolumeSet(jobworkerUuid, emptyVolumeConf, mockContext);
    emptyVolumeSet.initializeVolumeSet(clusterId);

    // Trying to create a task directory should throw exception
    String taskId = "test-task-" + UUID.randomUUID();
    Exception exception = assertThrows(IOException.class, () -> {
      emptyVolumeSet.createTaskDirectory(taskId);
    });

    assertTrue(exception.getMessage().contains("No viable volumes available"));
    emptyVolumeSet.close();
  }
}
