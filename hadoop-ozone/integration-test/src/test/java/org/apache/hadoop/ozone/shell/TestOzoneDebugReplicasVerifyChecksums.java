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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test Ozone Debug shell.
 */
@ExtendWith(DebugShellOzoneClusterExtension.class)
public class TestOzoneDebugReplicasVerifyChecksums {
  @RegisterExtension
  static DebugShellOzoneClusterExtension clusterExtension = new DebugShellOzoneClusterExtension();

  @TempDir
  private static File tempDir;
  private static final StringWriter out = new StringWriter();
  private static final StringWriter err = new StringWriter();

  private static OzoneShell ozoneShell;
  private static OzoneDebug ozoneDebugShell;
  private static OzoneClient client;
  private static ObjectStore store = null;
  private static String volume;
  private static String ozoneAddress;

  @BeforeAll
  static void setup() {
    store = clusterExtension.getStore();
    client = clusterExtension.getClient();
    ozoneShell = clusterExtension.getOzoneShell();
    ozoneDebugShell = clusterExtension.getOzoneDebugShell();
    ozoneDebugShell.getCmd().setOut(new PrintWriter(out));
    ozoneDebugShell.getCmd().setErr(new PrintWriter(err));
  }

  @BeforeAll
  static void setupKeys() throws IOException {
    volume = generateVolume("vol-a-");
    ozoneAddress = "/" + volume;
    generateKeys(volume, "level1/key-a-",10);
    generateKeys(volume, "key-b-",10);
  }

  @AfterEach
  void cleanupKeys() throws IOException {
//    ozoneShell.execute(new String[] {
//       "volume",
//        "delete",
//        ozoneAddress,
//        "-r",
//        "--yes"
//    });
//    for (Iterator<? extends OzoneBucket> it = store.getVolume(volume).listBuckets(null); it.hasNext();) {
//      OzoneBucket bucket = it.next();
//      store.getVolume(volume).deleteBucket(bucket.getName());
//    }
//    store.deleteVolume(volume);
  }

  private static String generateVolume(String volumePrefix) throws IOException {
    String volumeA = volumePrefix + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    return store.getVolume(volumeA).getName();
  }

  private static void generateKeys(String volume, String keyPrefix, int numberOfKeys) throws IOException {
    ReplicationConfig repConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    OzoneVolume volA = store.getVolume(volume);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    List<OzoneBucket> ozoneBuckets = new ArrayList<>();
    ozoneBuckets.add(volA.getBucket(bucketA));
    ozoneBuckets.add(volA.getBucket(bucketB));

    for (int i = 0; i < numberOfKeys; i++) {
      for (OzoneBucket bucket : ozoneBuckets) {
        byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
        String key = keyPrefix + i + "-" + RandomStringUtils.randomNumeric(5);
        TestDataUtil.createKey(bucket, key, repConfig, value);
      }
    }
  }

  public static Stream<Arguments> getTestChecksumsArguments() {
    return Stream.of(
        Arguments.of("case 1: test an invalid command", 2, new String[] {
            "replicas",
            "verify",
            "--checksums",
            ""}
        ),
        Arguments.of("case 2: test a valid command", 0, new String[] {
            "replicas",
            "verify",
            "--checksums",
            "--output-dir=" + tempDir.getAbsolutePath(),
            ""}
        )
    );
  }

  @MethodSource("getTestChecksumsArguments")
  @ParameterizedTest(name = "{0}")
  void testReplicas(String description, int expectedExitCode, String[] parameters) {
    parameters[parameters.length - 1] = ozoneAddress;
    int exitCode = ozoneDebugShell.execute(parameters);

    System.out.println(out);
    System.out.println(err);
    assertEquals(expectedExitCode, exitCode, err.toString());
  }
}
