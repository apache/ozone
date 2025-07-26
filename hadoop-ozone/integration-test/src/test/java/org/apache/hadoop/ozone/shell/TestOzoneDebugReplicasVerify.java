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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.TestDataUtil.createKeys;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.corruptFile;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.truncateFile;
import static org.apache.ozone.test.GenericTestUtils.setLogLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.apache.ratis.util.JvmPauseMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Test Ozone Debug Replicas Verify commands.
 */
public abstract class TestOzoneDebugReplicasVerify implements NonHATests.TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestOzoneDebugReplicasVerify.class);
  private static final String CHUNKS_DIR_NAME = "chunks";
  private static final String BLOCK_FILE_EXTENSION = ".block";

  private OzoneDebug ozoneDebugShell;
  private String ozoneAddress;
  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;
  private Map<String, OmKeyInfo> keyInfoMap;

  @BeforeEach
  void init() {
    setLogLevel(XceiverClientGrpc.class, Level.DEBUG);
    setLogLevel(JvmPauseMonitor.class, Level.ERROR);
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
    ozoneDebugShell = new OzoneDebug();
  }

  @BeforeEach
  void setupKeys() throws Exception {
    keyInfoMap = createKeys(cluster(), 10);
    ozoneAddress = "/" + keyInfoMap.get(keyInfoMap.keySet().stream().findAny().get()).getVolumeName();
  }

  @AfterEach
  void cleanupKeys() throws IOException {
    if (!out.toString().isEmpty()) {
      LOG.info(out.toString());
    }

    if (!err.toString().isEmpty()) {
      LOG.error(err.toString());
    }

    Set<String> volumeNames = keyInfoMap.values().stream()
        .map(OmKeyInfo::getVolumeName)
        .collect(Collectors.toSet());

    try (OzoneClient client = cluster().newClient()) {
      ObjectStore store = client.getObjectStore();
      for (Iterator<? extends OzoneVolume> volumeIterator
           = store.listVolumes(null); volumeIterator.hasNext();) {
        OzoneVolume ozoneVolume = volumeIterator.next();
        if (!volumeNames.contains(ozoneVolume.getName())) {
          continue;
        }
        for (Iterator<? extends OzoneBucket> bucketIterator
             = store.getVolume(ozoneVolume.getName()).listBuckets(null); bucketIterator.hasNext();) {
          OzoneBucket ozoneBucket = bucketIterator.next();
          for (Iterator<? extends OzoneKey> keyIterator
               = ozoneBucket.listKeys(null); keyIterator.hasNext();) {
            OzoneKey ozoneKey = keyIterator.next();
            ozoneBucket.deleteDirectory(ozoneKey.getName(), true);
          }
          ozoneVolume.deleteBucket(ozoneBucket.getName());
        }
        store.deleteVolume(ozoneVolume.getName());
      }
    }
  }

  /**
   * Generate string to pass as extra arguments to the
   * ozone debug command line, This is necessary for client to
   * connect to OM by setting the right om address.
   */
  private String getSetConfStringFromConf(String configKey) {
    return String.format("--set=%s=%s", configKey, cluster().getConf().get(configKey));
  }

  private Optional<File> findFirstBlockFile(Container<?> container, String fileName) {
    Objects.requireNonNull(container, "Container cannot be null");
    File chunksDir = new File(container.getContainerData().getContainerPath(), CHUNKS_DIR_NAME);
    Optional<File[]> files = Optional.ofNullable(chunksDir.listFiles((dir, name)
        -> name.contains(fileName) && name.endsWith(BLOCK_FILE_EXTENSION)));
    assertTrue(files.isPresent(), "No block files found in the container.");
    return Arrays.stream(files.get()).findFirst();
  }

  public void corruptBlock(Container<?> container, String fileName) {
    Optional<File> blockFile = findFirstBlockFile(container, fileName);
    assertTrue(blockFile.isPresent(), "No block file found in the container.");
    corruptFile(blockFile.get());
  }

  public void truncateBlock(Container<?> container, String fileName) {
    Optional<File> blockFile = findFirstBlockFile(container, fileName);
    assertTrue(blockFile.isPresent(), "No block file found in the container.");
    truncateFile(blockFile.get());
  }

  private Container<?> getFirstContainer(long containerID) {
    for (HddsDatanodeService dn : cluster().getHddsDatanodes()) {
      Container<?> container = dn.getDatanodeStateMachine()
          .getContainer()
          .getContainerSet()
          .getContainer(containerID);
      if (container != null) {
        return container;
      }
    }
    return null;
  }

  public static Stream<Arguments> getTestChecksumsArguments() {
    return Stream.of(
        Arguments.of("case 1: test missing checksums command", 2, Arrays.asList(
            "replicas",
            "verify")
        ),
        Arguments.of("case 2: test valid checksums command", 0, Arrays.asList(
            "replicas",
            "verify",
            "--checksums")
    ));
  }

  @MethodSource("getTestChecksumsArguments")
  @ParameterizedTest(name = "{0}")
  void testReplicas(String description, int expectedExitCode, List<String> parameters) {
    parameters = new ArrayList<>(parameters);
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add(ozoneAddress); // getTestChecksumsArguments is static. We cannot set the ozoneAddress there directly.

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));

    assertEquals(expectedExitCode, exitCode, err.toString());
    assertThat(out.get())
        .doesNotContain("Checksum mismatch")
        .doesNotContain("Unexpected read size");
  }

  @Test
  void testChecksumsWithCorruptedBlockFile() {
    Optional<String> key = keyInfoMap.keySet().stream().findAny();
    if (!key.isPresent()) {
      fail("No suitable key is available in the cluster");
    }
    OmKeyInfo keyInfo = keyInfoMap.get(key.get());
    OmKeyLocationInfo location = Objects.requireNonNull(keyInfo.getLatestVersionLocations()).getLocationList().get(0);
    Container<?> container = getFirstContainer(location.getContainerID());
    long localID = location.getLocalID();
    LOG.info("Corrupting key: {}/{}/{} with localID {}", keyInfoMap.get(key.get()).getVolumeName(),
        keyInfoMap.get(key.get()).getBucketName(), key.get(), localID);
    corruptBlock(container, Long.toString(localID));

    List<String> parameters = new ArrayList<>();
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add("replicas");
    parameters.add("verify");
    parameters.add("--checksums");
    parameters.add("--all-results");
    parameters.add(ozoneAddress);

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));

    assertEquals(0, exitCode, err.toString());
    assertThat(out.get())
        .contains("Checksum mismatch")
        .doesNotContain("Unexpected read size");
  }

  @Test
  void testChecksumsWithEmptyBlockFile() {
    Optional<String> key = keyInfoMap.keySet().stream().findAny();
    if (!key.isPresent()) {
      fail("No suitable key is available in the cluster");
    }
    OmKeyInfo keyInfo = keyInfoMap.get(key.get());
    OmKeyLocationInfo location = Objects.requireNonNull(keyInfo.getLatestVersionLocations()).getLocationList().get(0);
    Container<?> container = getFirstContainer(location.getContainerID());
    long localID = location.getLocalID();
    LOG.info("Truncating key: {} with localID {}", key, localID);
    truncateBlock(container, Long.toString(localID));

    List<String> parameters = new ArrayList<>();
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add("replicas");
    parameters.add("verify");
    parameters.add("--checksums");
    parameters.add("--all-results");
    parameters.add(ozoneAddress);

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));
    assertEquals(0, exitCode, err.get());
    assertThat(out.get())
        .contains("Unexpected read size")
        .doesNotContain("Checksum mismatch");
  }
}
