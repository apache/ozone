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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Ozone Debug shell.
 */
public abstract class TestOzoneDebugReplicasVerify implements NonHATests.TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestOzoneDebugReplicasVerify.class);

  @TempDir
  private static File tempDir;
  private static StringWriter out;
  private static StringWriter err;

  private static OzoneDebug ozoneDebugShell;
  private static OzoneClient client;
  private static ObjectStore store = null;
  private static String volume;
  private static String bucket;
  private static String key;
  private static String ozoneAddress;

  @BeforeAll
  public static void setup() throws IOException {
    LOG.info("Saving data to: {}", tempDir.getAbsolutePath());
  }

  @BeforeEach
  void init() throws Exception {
    out = new StringWriter();
    err = new StringWriter();
    ozoneDebugShell = new OzoneDebug();
    ozoneDebugShell.getCmd().setOut(new PrintWriter(out));
    ozoneDebugShell.getCmd().setErr(new PrintWriter(err));
    client = cluster().newClient();
    store = client.getObjectStore();
  }

  @BeforeEach
  void setupKeys() throws IOException {
    volume = generateVolume("vol-a-");
    ozoneAddress = "/" + volume;
    generateKeys(volume, "level1/multilevel-", 1);
    generateKeys(volume, "key-", 1);
  }

  @AfterEach
  void cleanupKeys() throws IOException {
    if (!out.toString().isEmpty()) {
      LOG.info(out.toString());
    }

    if (!err.toString().isEmpty()) {
      LOG.error(err.toString());
    }

    for (Iterator<? extends OzoneBucket> it = store.getVolume(volume).listBuckets(null); it.hasNext();) {
      OzoneBucket ozoneBucket = it.next();
      for (Iterator<? extends OzoneKey> keyIterator = ozoneBucket.listKeys(null); keyIterator.hasNext();) {
        OzoneKey ozoneKey = keyIterator.next();
        ozoneBucket.deleteDirectory(ozoneKey.getName(), true);
      }
      store.getVolume(volume).deleteBucket(ozoneBucket.getName());
    }
    store.deleteVolume(volume);
  }

  /**
   * Generate string to pass as extra arguments to the
   * ozone debug command line, This is necessary for client to
   * connect to OM by setting the right om address.
   */
  private String getSetConfStringFromConf(String configKey) {
    return String.format("--set=%s=%s", configKey, cluster().getConf().get(configKey));
  }


  public void corruptBlock(Container<?> container) {
    File chunksDir = new File(container.getContainerData().getContainerPath(), "chunks");
    Optional<File> blockFile = Arrays.stream(Objects.requireNonNull(
        chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
        .findFirst();
    assertTrue(blockFile.isPresent());
    corruptFile(blockFile.get());
  }

  public void truncateBlock(Container<?> container) {
    File chunksDir = new File(container.getContainerData().getContainerPath(), "chunks");
    Optional<File> blockFile = Arrays.stream(Objects.requireNonNull(
            chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
        .findFirst();
    assertTrue(blockFile.isPresent());
    truncateFile(blockFile.get());
  }

  /**
   * Overwrite the file with random bytes.
   */
  private static void corruptFile(File file) {
    LOG.info("Corrupting file: {}", file.getAbsolutePath());
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

  private Container<?> getFirstKeyContainer() throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .build();
    OmKeyInfo keyInfo = cluster().getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assertions.assertNotNull(locations);
    long containerID = locations.getLocationList().get(0).getContainerID();

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

  private static String generateVolume(String volumePrefix) throws IOException {
    String volumeA = volumePrefix + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    return store.getVolume(volumeA).getName();
  }

  private static void generateKeys(String volumeName, String keyPrefix, int numberOfKeys) throws IOException {
    ReplicationConfig repConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE);
    bucket = "bucket-" + RandomStringUtils.randomNumeric(5);
    OzoneVolume volA = store.getVolume(volumeName);
    volA.createBucket(bucket);

    for (int i = 0; i < numberOfKeys; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      String k = keyPrefix + i + "-" + RandomStringUtils.randomNumeric(5);
      key = k;
      TestDataUtil.createKey(volA.getBucket(bucket), k, repConfig, value);
    }
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
  void testReplicas(String description, int expectedExitCode, List<String> parameters) throws IOException {
    Path checksumsOutputDir = tempDir.toPath().resolve(RandomStringUtils.insecure().nextAlphanumeric(10));
    parameters = new ArrayList<>(parameters);
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add("--output-dir=" + checksumsOutputDir);
    parameters.add(ozoneAddress); // getTestChecksumsArguments is static. We cannot set the ozoneAddress there directly.

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));

    assertEquals(expectedExitCode, exitCode, err.toString());
    if (checksumsOutputDir.toFile().exists()) {
      Files.walk(checksumsOutputDir)
          .filter(Files::isRegularFile)
          .map(e -> {
            try {
              return new String(Files.readAllBytes(e.toAbsolutePath()));
            } catch (IOException ignored) {
            }
            return "";
          })
          .forEach(manifestFile -> assertThat(manifestFile)
              .doesNotContain("Checksum mismatch")
              .doesNotContain("Unexpected read size"));
    }
  }

  @Test
  void testChecksumsWithCorruptedBlockFile() throws IOException {
    Path checksumsOutputDir = tempDir.toPath().resolve(RandomStringUtils.insecure().nextAlphanumeric(10));
    Container<?> container = getFirstKeyContainer();
    LOG.info("Corrupting key: /" + volume + "/" + bucket + "/" + key);
    corruptBlock(container);

    List<String> parameters = new ArrayList<>();
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add("replicas");
    parameters.add("verify");
    parameters.add("--checksums");
    parameters.add("--output-dir=" + checksumsOutputDir);
    parameters.add(ozoneAddress);

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));

    assertEquals(0, exitCode, err.toString());
    if (checksumsOutputDir.toFile().exists()) {
      Files.walk(checksumsOutputDir)
          .filter(Files::isRegularFile)
          .map(e -> {
            try {
              return new String(Files.readAllBytes(e.toAbsolutePath()));
            } catch (IOException ignored) {
            }
            return "";
          })
          .forEach(manifestFile -> {
            if (manifestFile.contains(key)) {
              assertThat(manifestFile).contains("Checksum mismatch");
            } else {
              assertThat(manifestFile)
                  .doesNotContain("Checksum mismatch")
                  .doesNotContain("Unexpected read size");
            }
          });
    }
  }

  @Test
  void testChecksumsWithEmptyBlockFile() throws IOException {
    Path checksumsOutputDir = tempDir.toPath().resolve(RandomStringUtils.insecure().nextAlphanumeric(10));
    Container<?> container = getFirstKeyContainer();
    LOG.info("Corrupting key: /" + volume + "/" + bucket + "/" + key);
    truncateBlock(container);

    List<String> parameters = new ArrayList<>();
    parameters.add(0, getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY));
    parameters.add(0, getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY));
    parameters.add("replicas");
    parameters.add("verify");
    parameters.add("--checksums");
    parameters.add("--output-dir=" + checksumsOutputDir);
    parameters.add(ozoneAddress);

    int exitCode = ozoneDebugShell.execute(parameters.toArray(new String[0]));

    assertEquals(0, exitCode, err.toString());
    if (checksumsOutputDir.toFile().exists()) {
      Files.walk(checksumsOutputDir)
          .filter(Files::isRegularFile)
          .map(e -> {
            try {
              return new String(Files.readAllBytes(e.toAbsolutePath()));
            } catch (IOException ignored) {
            }
            return "";
          })
          .forEach(manifestFile -> {
            if (manifestFile.contains(key)) {
              assertThat(manifestFile).contains("Unexpected read size");
            } else {
              assertThat(manifestFile)
                  .doesNotContain("Checksum mismatch")
                  .doesNotContain("Unexpected read size");
            }
          });
    }
  }
}
