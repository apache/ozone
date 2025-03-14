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

package org.apache.hadoop.ozone.dn.scanner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests the data scanner functionality.
 */
@Timeout(300)
public abstract class TestContainerScannerIntegrationAbstract {

  private static MiniOzoneCluster cluster;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  protected static final Duration SCAN_INTERVAL = Duration.ofSeconds(1);
  private static String volumeName;
  private static String bucketName;
  private static OzoneBucket bucket;

  public static void buildCluster(OzoneConfiguration ozoneConfig)
      throws Exception {
    // Allow SCM to quickly learn about the unhealthy container.
    ozoneConfig.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    // Speed up corruption detection by allowing scans of the same container to
    // run back to back.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.CONTAINER_SCAN_MIN_GAP,
        0, TimeUnit.SECONDS);

    // Build a one datanode cluster.
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);
    ozClient = OzoneClientFactory.getRpcClient(ozoneConfig);
    store = ozClient.getObjectStore();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  void pauseScanner() {
    getOzoneContainer().pauseContainerScrub();
  }

  void resumeScanner() {
    getOzoneContainer().resumeContainerScrub();
  }


  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected void waitForScmToSeeUnhealthyReplica(long containerID)
      throws Exception {
    ContainerManager scmContainerManager = cluster.getStorageContainerManager()
        .getContainerManager();
    LambdaTestUtils.await(5000, 500,
        () -> getContainerReplica(scmContainerManager, containerID)
            .getState() == State.UNHEALTHY);
  }

  protected void waitForScmToCloseContainer(long containerID) throws Exception {
    ContainerManager cm = cluster.getStorageContainerManager()
        .getContainerManager();
    LambdaTestUtils.await(5000, 500,
        () -> cm.getContainer(ContainerID.valueOf(containerID)).getState()
            != HddsProtos.LifeCycleState.OPEN);
  }

  private static OzoneContainer getOzoneContainer() {
    assertEquals(1, cluster.getHddsDatanodes().size());
    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    return dn.getDatanodeStateMachine().getContainer();
  }

  protected Container<?> getDnContainer(long containerID) {
    return getOzoneContainer().getContainerSet().getContainer(containerID);
  }

  protected long writeDataThenCloseContainer() throws Exception {
    return writeDataThenCloseContainer("keyName");
  }

  protected long writeDataThenCloseContainer(String keyName) throws Exception {
    OzoneOutputStream key = createKey(keyName);
    key.write(getTestData());
    key.flush();
    key.close();

    long containerID = bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
    closeContainerAndWait(containerID);
    return containerID;
  }

  protected void closeContainerAndWait(long containerID) throws Exception {
    cluster.getStorageContainerLocationClient().closeContainer(containerID);

    GenericTestUtils.waitFor(
        () -> TestHelper.isContainerClosed(cluster, containerID,
            cluster.getHddsDatanodes().get(0).getDatanodeDetails()),
        1000, 5000);
  }

  protected long writeDataToOpenContainer() throws Exception {
    String keyName = "keyName";
    OzoneOutputStream key = createKey(keyName);
    key.write(getTestData());
    key.close();

    return bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
  }

  protected byte[] getTestData() {
    int sizeLargerThanOneChunk = (int) (OzoneConsts.MB + OzoneConsts.MB / 2);
    return ContainerTestHelper
        .getFixedLengthString("sample value", sizeLargerThanOneChunk)
        .getBytes(UTF_8);
  }

  protected ContainerReplica getContainerReplica(
      ContainerManager cm, long containerId) throws ContainerNotFoundException {
    Set<ContainerReplica> containerReplicas = cm.getContainerReplicas(
        ContainerID.valueOf(
            containerId));
    // Only using a single datanode cluster.
    assertEquals(1, containerReplicas.size());
    return containerReplicas.iterator().next();
  }

  //ignore the result of the key read because it is expected to fail
  @SuppressWarnings("ResultOfMethodCallIgnored")
  protected void readFromCorruptedKey(String keyName) throws IOException {
    try (OzoneInputStream key = bucket.readKey(keyName)) {
      assertThrows(IOException.class, key::read);
    }
  }

  private OzoneOutputStream createKey(String keyName) throws Exception {
    return TestHelper.createKey(
        keyName, RATIS, ONE, 0, store, volumeName, bucketName);
  }

  /**
   * Represents a type of container corruption that can be injected into the
   * test.
   */
  protected enum ContainerCorruptions {
    MISSING_CHUNKS_DIR(container -> {
      File chunksDir = new File(container.getContainerData().getContainerPath(),
          "chunks");
      try {
        FileUtils.deleteDirectory(chunksDir);
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
      assertFalse(chunksDir.exists());
    }, ScanResult.FailureType.MISSING_CHUNKS_DIR),

    MISSING_METADATA_DIR(container -> {
      File metadataDir =
          new File(container.getContainerData().getContainerPath(),
              "metadata");
      try {
        FileUtils.deleteDirectory(metadataDir);
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
      assertFalse(metadataDir.exists());
    }, ScanResult.FailureType.MISSING_METADATA_DIR),

    MISSING_CONTAINER_FILE(container -> {
      File containerFile = container.getContainerFile();
      assertTrue(containerFile.delete());
      assertFalse(containerFile.exists());
    }, ScanResult.FailureType.MISSING_CONTAINER_FILE),

    MISSING_CONTAINER_DIR(container -> {
      File containerDir =
          new File(container.getContainerData().getContainerPath());
      try {
        FileUtils.deleteDirectory(containerDir);
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
      assertFalse(containerDir.exists());
    }, ScanResult.FailureType.MISSING_CONTAINER_DIR),

    MISSING_BLOCK(container -> {
      File chunksDir = new File(
          container.getContainerData().getContainerPath(), "chunks");
      for (File blockFile:
          chunksDir.listFiles((dir, name) -> name.endsWith(".block"))) {
        try {
          Files.delete(blockFile.toPath());
        } catch (IOException ex) {
          // Fail the test.
          throw new UncheckedIOException(ex);
        }
      }
    }, ScanResult.FailureType.MISSING_CHUNK_FILE),

    CORRUPT_CONTAINER_FILE(container -> {
      File containerFile = container.getContainerFile();
      corruptFile(containerFile);
    }, ScanResult.FailureType.CORRUPT_CONTAINER_FILE),

    TRUNCATED_CONTAINER_FILE(container -> {
      File containerFile = container.getContainerFile();
      truncateFile(containerFile);
    }, ScanResult.FailureType.CORRUPT_CONTAINER_FILE),

    CORRUPT_BLOCK(container -> {
      File chunksDir = new File(container.getContainerData().getContainerPath(),
          "chunks");
      Optional<File> blockFile = Arrays.stream(Objects.requireNonNull(
              chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
          .findFirst();
      assertTrue(blockFile.isPresent());
      corruptFile(blockFile.get());
    }, ScanResult.FailureType.CORRUPT_CHUNK),

    TRUNCATED_BLOCK(container -> {
      File chunksDir = new File(container.getContainerData().getContainerPath(),
          "chunks");
      Optional<File> blockFile = Arrays.stream(Objects.requireNonNull(
              chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
          .findFirst();
      assertTrue(blockFile.isPresent());
      truncateFile(blockFile.get());
    }, ScanResult.FailureType.INCONSISTENT_CHUNK_LENGTH);

    private final Consumer<Container<?>> corruption;
    private final ScanResult.FailureType expectedResult;

    ContainerCorruptions(Consumer<Container<?>> corruption,
                         ScanResult.FailureType expectedResult) {
      this.corruption = corruption;
      this.expectedResult = expectedResult;

    }

    public void applyTo(Container<?> container) {
      corruption.accept(container);
    }

    /**
     * Check that the correct corruption type was written to the container log.
     */
    public void assertLogged(LogCapturer logCapturer) {
      assertThat(logCapturer.getOutput())
          .contains(expectedResult.toString());
    }

    /**
     * Get all container corruption types as parameters for junit 4
     * parameterized tests, except the ones specified.
     */
    public static Set<ContainerCorruptions> getAllParamsExcept(
        ContainerCorruptions... exclude) {
      Set<ContainerCorruptions> includeSet =
          EnumSet.allOf(ContainerCorruptions.class);
      Arrays.asList(exclude).forEach(includeSet::remove);
      return includeSet;
    }

    /**
     * Overwrite the file with random bytes.
     */
    private static void corruptFile(File file) {
      try {
        final int length = (int) file.length();

        Path path = file.toPath();
        final byte[] original = IOUtils.readFully(Files.newInputStream(path), length);

        final byte[] corruptedBytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(corruptedBytes);

        Files.write(path, corruptedBytes,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

        assertThat(IOUtils.readFully(Files.newInputStream(path), length))
            .isEqualTo(corruptedBytes)
            .isNotEqualTo(original);
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
    }

    /**
     * Truncate the file to 0 bytes in length.
     */
    private static void truncateFile(File file) {
      try {
        Files.write(file.toPath(), new byte[0],
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

        assertEquals(0, file.length());
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
    }
  }
}
