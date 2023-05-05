/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn.scanner;

import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerMetadataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerDataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;

/**
 * This class tests the data scanner functionality.
 */
public abstract class TestContainerScannersAbstract {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static MiniOzoneCluster cluster;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  protected static final Duration SCAN_INTERVAL = Duration.ofSeconds(3);
  private static String volumeName;
  private static String bucketName;
  private static OzoneBucket bucket;

  public static void buildCluster(OzoneConfiguration ozoneConfig) throws Exception {
    // Set general cluster configurations.
    ozoneConfig.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");

    // Set scanner related configurations.
    ozoneConfig.setTimeDuration(ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    ozoneConfig.setTimeDuration(ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);

    // Enable only the scanner under test.

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

  @AfterClass
  public static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * {@link BackgroundContainerMetadataScanner} should detect metadata
   * corruption on open and closed containers.
   */

  /**
   * {@link OnDemandContainerDataScanner} should detect corrupted blocks
   * in a closed container after client interaction.
   * {@link OnDemandContainerDataScanner} should detect corrupted blocks
   * in an open container after client interaction fails and SCM closes the
   * container.
   */

  protected void waitForScmToSeeUnhealthy(long containerID) throws Exception {
    ContainerManager scmContainerManager = cluster.getStorageContainerManager()
        .getContainerManager();
    LambdaTestUtils.await(1, 1,
        () -> getContainerReplica(scmContainerManager, containerID)
            .getState() == State.UNHEALTHY);
  }

  protected Container<?> getContainer(long containerID) {
    Assert.assertEquals(1, cluster.getHddsDatanodes().size());
    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    return oc.getContainerSet().getContainer(containerID);
  }

  protected long writeDataThenCloseContainer() throws Exception {
    String keyName = "keyName";
    OzoneOutputStream key = createKey(volumeName, bucketName,
        keyName);
    key.write(getTestData());
    key.flush();
    TestHelper.waitForContainerClose(key, cluster);
    key.close();

    return bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
  }

  protected long writeDataToOpenContainer() throws Exception {
    String keyName = "keyName";
    OzoneOutputStream key = createKey(volumeName, bucketName,
        keyName);
    key.write(getTestData());
    key.close();

    return bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
  }

  private byte[] getTestData() {
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
    Assert.assertEquals(1, containerReplicas.size());
    return containerReplicas.iterator().next();
  }

  //ignore the result of the key read because it is expected to fail
  @SuppressWarnings("ResultOfMethodCallIgnored")
  protected void readFromCorruptedKey(
      OzoneBucket bucket, String keyName) throws IOException {
    try (OzoneInputStream key = bucket.readKey(keyName)) {
      Assert.assertThrows(IOException.class, key::read);
    }
  }

//  protected Container<?> getContainerInState(
//      ContainerSet cs, ContainerProtos.ContainerDataProto.State state) {
//    return cs.getContainerMap().values().stream()
//        .filter(c -> state ==
//            c.getContainerState())
//        .findAny()
//        .orElseThrow(() ->
//            new RuntimeException("No Open container found for testing"));
//  }

  protected OzoneOutputStream createKey(String volumeName, String bucketName,
                                      String keyName) throws Exception {
    return TestHelper.createKey(
        keyName, RATIS, ONE, 0, store, volumeName, bucketName);
  }

  /**
   * Represents a type of container corruption that can be injected into the
   * test.
   */
  protected interface ContainerCorruption {
    void applyTo(Container<?> container) throws IOException;
  }

  protected static final ContainerCorruption MISSING_CHUNKS_DIR = container -> {
    File chunksDir = new File(container.getContainerData().getContainerPath(), "chunks");
    FileUtils.deleteDirectory(chunksDir);
    Assert.assertFalse(chunksDir.exists());
  };

  protected static final ContainerCorruption MISSING_METADATA_DIR = container -> {
    File metadataDir = new File(container.getContainerData().getContainerPath(), "metadata");
    FileUtils.deleteDirectory(metadataDir);
    Assert.assertFalse(metadataDir.exists());
  };

  protected static final ContainerCorruption MISSING_CONTAINER_FILE = container -> {
    // TODO get this correctly.
    File containerFile = new File(container.getContainerData().getContainerPath(), "metadata");
    Assert.assertTrue(containerFile.delete());
    Assert.assertFalse(containerFile.exists());
  };

  protected static final ContainerCorruption MISSING_CONTAINER_DIR = container -> {
    File containerDir =
        new File(container.getContainerData().getContainerPath());
    FileUtils.deleteDirectory(containerDir);
    Assert.assertFalse(containerDir.exists());
  };

  protected static final ContainerCorruption MISSING_BLOCK = container -> {
    File chunksDir = new File(container.getContainerData().getContainerPath(), "chunks");

    for (File blockFile:
        chunksDir.listFiles((dir, name) -> name.endsWith(".block"))) {
      corruptFile(blockFile);
    }
  };

  protected static final ContainerCorruption CORRUPT_CONTAINER_FILE = container -> {
    // TODO get this correctly.
    File containerFile = new File(container.getContainerData().getContainerPath(), "metadata");
    corruptFile(containerFile);
  };

  protected static final ContainerCorruption CORRUPT_BLOCK = container -> {
    File chunksDir = new File(container.getContainerData().getContainerPath(), "chunks");

    Optional<File> blockFile = Arrays.stream(Objects.requireNonNull(
        chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
        .findFirst();
    Assert.assertTrue(blockFile.isPresent());
    corruptFile(blockFile.get());
  };

  private static void corruptFile(File file) throws IOException {
    byte[] corruptedBytes = new byte[(int)file.length()];
    new Random().nextBytes(corruptedBytes);
    Files.write(file.toPath(), corruptedBytes,
        StandardOpenOption.TRUNCATE_EXISTING);
  }
}
