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

package org.apache.hadoop.ozone.debug;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConsts.MD5_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.debug.replicas.ReplicasDebug;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestReplicasCli extends OzoneTestBase {
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB storageContainerLocationClient;
  private static MessageDigest eTagProvider;
  private static Set<OzoneClient> ozoneClients = new HashSet<>();
  private static GenericTestUtils.PrintStreamCapturer output;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;

  @BeforeAll
  public static void initialize() throws Exception {
    eTagProvider = MessageDigest.getInstance(MD5_HASH);
    AuditLogTestUtils.enableAuditLog();
    output = GenericTestUtils.captureOut();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS, OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
  }

  @BeforeEach
  public void setup() throws IOException {
    stdout = new StringWriter();
    pstdout = new PrintWriter(stdout);
    stderr = new StringWriter();
    pstderr = new PrintWriter(stderr);

    cmd = new CommandLine(new ReplicasDebug())
        .setOut(pstdout)
        .setErr(pstderr);
  }

  @AfterEach
  public void shutdown() throws IOException {
    pstderr.close();
    stderr.close();
    pstdout.close();
    stdout.close();
  }

  @AfterAll
  public static void teardown() throws IOException {
    shutdownCluster();
  }

  /**
   * Create a MiniOzoneCluster for testing.
   * @param conf Configurations to start the cluster.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    startCluster(conf, MiniOzoneCluster.newBuilder(conf));
  }

  static void startCluster(OzoneConfiguration conf, MiniOzoneCluster.Builder builder) throws Exception {
    // Reduce long wait time in MiniOzoneClusterImpl#waitForHddsDatanodesStop
    //  for testZReadKeyWithUnhealthyContainerReplica.
    conf.set("ozone.scm.stale.node.interval", "1s");
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    ClientConfigForTesting.newBuilder(StorageUnit.MB)
        .setDataStreamMinPacketSize(1)
        .applyTo(conf);

    cluster = builder
        .setNumDatanodes(14)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    ozoneClients.add(ozClient);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  static void shutdownCluster() {
    org.apache.hadoop.hdds.utils.IOUtils.closeQuietly(ozoneClients);
    ozoneClients.clear();
    org.apache.hadoop.hdds.utils.IOUtils.closeQuietly(output);

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneKeyDetails createTestKey(OzoneBucket bucket) throws IOException {
    return createTestKey(bucket, getTestName(), UUID.randomUUID().toString());
  }

  private OzoneKeyDetails createTestKey(
      OzoneBucket bucket, String keyName, String keyValue
  ) throws IOException {
    return createTestKey(bucket, keyName, keyValue.getBytes(UTF_8));
  }

  private OzoneKeyDetails createTestKey(
      OzoneBucket bucket, String keyName, byte[] bytes
  ) throws IOException {
    RatisReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
    Map<String, String> metadata = singletonMap("key", RandomStringUtils.randomAscii(10));
    try (OzoneOutputStream out = bucket.createKey(keyName, bytes.length, replication, metadata)) {
      out.write(bytes);
    }
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertNotNull(key);
    assertEquals(keyName, key.getName());
    return key;
  }

  private String generateKeys()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
    }

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "level1/key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
    }

    Iterator<? extends OzoneKey> volABucketAIter = volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while (volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    assertEquals(10, volABucketAKeyCount);
    Iterator<? extends OzoneKey> volABucketBIter = volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while (volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    assertEquals(10, volABucketBKeyCount);


    Iterator<? extends OzoneKey> volABucketAKeyAIter = volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while (volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter = volAbucketA.listKeys("level1");
    volABucketAKeyBIter.next();
    for (int i = 0; i < 10; i++) {
      OzoneKey key = volABucketAKeyBIter.next();
      assertTrue(key.getName().startsWith("level1/key-b-" + i + "-"));
    }
    assertFalse(volABucketBIter.hasNext());

    return "/" + volumeA+ "/" + bucketA;
  }

  @Test
  public void testReplicas() throws Exception {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
    }

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "level1/key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
    }

    String path = "/";
    // Prepare scan args
    List<String> completeScanArgs = new ArrayList<>();
    completeScanArgs.addAll(Arrays.asList(
        "verify",
        "--checksums",
        "--output-dir", tempDir.getAbsolutePath(),
        path
    ));

    int exitCode = cmd.execute(completeScanArgs.toArray(new String[0]));
    assertEquals(0, exitCode, stderr.toString());
    System.out.println(stdout);
  }
}

//TODO: org.apache.hadoop.ozone.dn.scanner.TestContainerScannerIntegrationAbstract.ContainerCorruptions.corruptFile
// see this class to corrupt the blocks and containers and see what type of exceptions are thrown


