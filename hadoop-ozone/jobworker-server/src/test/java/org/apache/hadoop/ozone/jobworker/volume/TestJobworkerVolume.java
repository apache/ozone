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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link VolatileJobworkerVolume}.
 */
public class TestJobworkerVolume {

  private static final String JOBWORKER_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @TempDir
  private Path folder;

  private VolatileJobworkerVolume.Builder volumeBuilder;
  private File versionFile;
  private File rootDir;

  @BeforeEach
  public void setup() throws Exception {
    rootDir = folder.toFile();
    volumeBuilder = new VolatileJobworkerVolume.Builder(rootDir.getAbsolutePath())
        .conf(CONF)
        .jobWorkerUuid(JOBWORKER_UUID)
        .clusterID(CLUSTER_ID);
  }

  @Test
  public void testReadPropertiesFromVersionFile() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();

    // Version file should be created automatically during initialization
    versionFile = volume.getVersionFile();
    assertTrue(versionFile.exists(), "Version file should be created during initialization");

    Properties properties = JobworkerVersionFile.readFrom(versionFile);
    String currentPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    String storageID = properties.getProperty(OzoneConsts.STORAGE_ID);
    String clusterID = properties.getProperty(OzoneConsts.CLUSTER_ID);
    String jobworkerUuid = properties.getProperty(OzoneConsts.JOBWORKER_UUID);
    long cTime = Long.parseLong(properties.getProperty(OzoneConsts.CTIME));
    int layoutVersion = Integer.parseInt(properties.getProperty(OzoneConsts.LAYOUTVERSION));
    String jobworkerPid = properties.getProperty(OzoneConsts.PID);

    assertEquals(volume.getStorageID(), storageID);
    assertEquals(volume.getClusterID(), clusterID);
    assertEquals(volume.getJobworkerUuid(), jobworkerUuid);
    assertTrue(cTime > 0);
    assertEquals(JobworkerVolumeLayoutVersion.getLatestVersion().getVersion(), layoutVersion);
    assertEquals(currentPid, jobworkerPid);

    // Cleanup
    volume.shutdown();
  }

  @Test
  public void testJobworkerVolumeInitialization() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();

    // The state of JobworkerVolume after initialization should be NORMAL
    assertEquals(VolatileJobworkerVolume.VolumeState.NORMAL, volume.getState());

    File jobworkerDir = volume.getJobworkerDir();
    assertTrue(jobworkerDir.exists(), "JobworkerVolume directory should be created");
    assertTrue(volume.getVersionFile().exists(),
        "Version file should exist after initialization");
    // ClusterDir and storage directories should be created
    assertTrue(volume.getClusterDir().exists(), "Cluster directory should be created");
    assertTrue(volume.getJobworkerRootDir().exists(), "Jobworker directory should be created");
    assertTrue(volume.getDiskCheckDir().exists(), "Disk check directory should be created");

    volume.shutdown();

    // After shutdown, the volume state should be NOT_FORMATTED
    assertEquals(VolatileJobworkerVolume.VolumeState.NOT_FORMATTED, volume.getState());
  }

  @Test
  public void testCreateTaskDirectory() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();

    String taskId = "task-" + UUID.randomUUID();
    File taskDir = volume.createTaskDirectory(taskId);

    assertTrue(taskDir.exists(), "Task directory should be created");
    assertTrue(taskDir.isDirectory(), "Task directory should be a directory");
    File currentDir = volume.getCurrentDir();
    assertEquals(taskDir.getParent(), currentDir.toString(),
        "Task directory should be under the current directory");

    volume.shutdown();
  }

  @Test
  public void testCleanupTaskDirectory() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();
    String taskId = "task-" + UUID.randomUUID();
    File taskDir = volume.createTaskDirectory(taskId);

    // Create some files in the task directory
    File dataFile = new File(taskDir, "data.txt");
    Files.write(dataFile.toPath(), "test data".getBytes());
    // Cleanup should remove the task directory
    boolean result = volume.cleanupTaskDirectory(taskId);

    assertTrue(result, "Cleanup should succeed");
    assertFalse(taskDir.exists(), "Task directory should be removed after cleanup");
    // Clean up a non-existent task should return true (no-op)
    result = volume.cleanupTaskDirectory("non-existent-task");
    assertTrue(result, "Cleanup of non-existent task should succeed");

    volume.shutdown();
  }

  @Test
  public void testFailedCreateATaskDirMultiple() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();
    String taskId = "task-" + UUID.randomUUID();
    volume.createTaskDirectory(taskId);
    Exception exception = assertThrows(IOException.class, () -> {
      volume.createTaskDirectory(taskId);
    });
    assertTrue(exception.getMessage().contains("Unable to create the task directory"));
    volume.shutdown();
  }

  @Test
  public void testFailedVolume() throws Exception {
    // Create a failed volume
    VolatileJobworkerVolume volume = volumeBuilder.failedVolume(true).build();

    // State should be FAILED
    assertEquals(VolatileJobworkerVolume.VolumeState.FAILED, volume.getState());
    assertTrue(volume.isFailed(), "Volume should be marked as failed");

    String taskId = "task-" + UUID.randomUUID();
    Exception exception = assertThrows(IOException.class, () -> {
      volume.createTaskDirectory(taskId);
    });
    assertTrue(exception.getMessage().contains("not in NORMAL state"));
    assertFalse(volume.cleanupTaskDirectory(taskId));

    volume.shutdown();
  }

  @Test
  public void testVolumeCapacityAndUsage() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();

    // Capacity and available should reflect the underlying filesystem
    assertTrue(volume.getCapacity() > 0, "Capacity should be positive");
    assertTrue(volume.getAvailable() > 0, "Available space should be positive");

    // Create some task directories to consume space
    for (int i = 0; i < 5; i++) {
      String taskId = "task-" + i;
      File taskDir = volume.createTaskDirectory(taskId);
      File largeFile = new File(taskDir, "large-file.bin");
      byte[] data = new byte[1024 * 1024]; // 1MB
      Files.write(largeFile.toPath(), data);
    }

    // Space usage should update
    assertTrue(volume.getCapacity() >= volume.getAvailable());
    volume.shutdown();
  }

  @Test
  public void testShutdownCleansUp() throws Exception {
    VolatileJobworkerVolume volume = volumeBuilder.build();
    String taskId = "task-" + UUID.randomUUID();
    File taskDir = volume.createTaskDirectory(taskId);
    File dataFile = new File(taskDir, "data.txt");
    Files.write(dataFile.toPath(), "test data".getBytes());
    File jobworkerDir = volume.getJobworkerDir();
    File localVersionFile = volume.getVersionFile();
    File clusterDir = volume.getClusterDir();
    File diskCheckDir = volume.getDiskCheckDir();
    File volumeRoot = volume.getVolumeRootDir();
    File jobworkerRootDir = volume.getJobworkerRootDir();

    // Verify directories exist before shutdown
    assertTrue(jobworkerDir.exists(), "Jobworker directory should exist");
    assertTrue(localVersionFile.exists(), "Version file should exist");
    assertTrue(clusterDir.exists(), "Cluster directory should exist");
    assertTrue(diskCheckDir.exists(), "Disk check directory should exist");
    assertTrue(taskDir.exists(), "Task directory should exist");
    assertTrue(volumeRoot.exists(), "VolumeRoot directory should exist");
    assertTrue(jobworkerRootDir.exists(), "jobworkerRoot directory should exist");

    volume.shutdown();

    // After shutdown, directories should be removed
    assertEquals(VolatileJobworkerVolume.VolumeState.NOT_FORMATTED, volume.getState());
    assertFalse(jobworkerDir.exists(), "Jobworker directory should not exist");
    assertFalse(localVersionFile.exists(), "Version file should not exist");
    assertFalse(clusterDir.exists(), "Cluster directory should not exist");
    assertFalse(diskCheckDir.exists(), "Disk check directory should not exist");
    // Task directory should be deleted, even though we do not manually release the task directory
    assertFalse(taskDir.exists(), "Task directory should not exist");
    // Volume root and jobworker root should not be deleted
    assertTrue(jobworkerRootDir.exists(), "jobworkerRoot directory should exist");
    assertTrue(volumeRoot.exists(), "Task directory should exist");
  }

  @Test
  public void testInitFailureWithNoPermissionDir() throws Exception {
    boolean unWritable = rootDir.setWritable(false);
    try {
      VolatileJobworkerVolume.Builder builder = new VolatileJobworkerVolume.Builder(rootDir.getAbsolutePath())
          .conf(CONF)
          .jobWorkerUuid(JOBWORKER_UUID)
          .clusterID(CLUSTER_ID);

      Exception exception = assertThrows(Exception.class, builder::build);

      assertTrue(exception.getMessage().contains("Cannot create the directory"));
    } finally {
      if (unWritable) {
        rootDir.setWritable(true);
      }
    }
  }

  @Test
  public void testInitFailureWithNullClusterId() throws Exception {
    // Create a builder with null clusterId
    VolatileJobworkerVolume.Builder builder = new VolatileJobworkerVolume.Builder(rootDir.getAbsolutePath())
        .conf(CONF)
        .jobWorkerUuid(JOBWORKER_UUID)
        .clusterID(null);

    Exception exception = assertThrows(Exception.class, builder::build);

    assertTrue(exception.getMessage().contains("ClusterID cannot be null"));
  }

  @Test
  public void testInitFailureWithNullJobworkerUuid() throws Exception {
    // Create a builder with null jobworkerUuid
    VolatileJobworkerVolume.Builder builder = new VolatileJobworkerVolume.Builder(rootDir.getAbsolutePath())
        .conf(CONF)
        .jobWorkerUuid(null)
        .clusterID(CLUSTER_ID);

    Exception exception = assertThrows(Exception.class, builder::build);

    assertTrue(exception.getMessage().contains("JobWorkerUUID cannot be null"));
  }

}
