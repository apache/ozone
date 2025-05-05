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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the order of Replication config resolution.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestReplicationConfigPreference implements NonHATests.TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationConfigPreference.class);

  private static final String VOLUME_NAME = "vol-" + UUID.randomUUID();
  private static final List<ReplicationConfig> REPLICATIONS = Arrays.asList(
      null,
      RatisReplicationConfig.getInstance(THREE),
      new ECReplicationConfig(3, 2)
  );
  private static final List<String> CONFIG_KEYS = Arrays.asList(
      OZONE_REPLICATION, OZONE_REPLICATION_TYPE,
      OZONE_SERVER_DEFAULT_REPLICATION_KEY, OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY
  );

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private File testFile;
  private final Map<String, String> originalSettings = new HashMap<>();
  private OzoneVolume volume;

  @BeforeAll
  void init(@TempDir Path path) throws Exception {
    cluster = cluster();
    client = cluster.newClient();

    testFile = path.resolve("testFile").toFile();
    FileUtils.createParentDirectories(testFile);
    FileUtils.touch(testFile);

    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    originalSettings.clear();
    for (String key : CONFIG_KEYS) {
      originalSettings.put(key, conf.get(key));
      conf.unset(key);
    }

    TestDataUtil.createVolume(client, VOLUME_NAME);
    volume = client.getObjectStore().getVolume(VOLUME_NAME);
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);

    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    originalSettings.forEach((k, v) -> TestHelper.setConfig(conf, k, v));
  }

  private static void execute(OzoneShell shell, List<String> args) {
    LOG.info("ozone sh {}", String.join(" ", args));
    shell.getCmd().execute(args.toArray(new String[0]));
  }

  private void createBucket(OzoneShell ozoneShell,
      String bucketName,
      ReplicationConfig replicationConfig
  ) {
    List<String> args = new ArrayList<>(Arrays.asList(
        "bucket", "create",
        VOLUME_NAME + "/" + bucketName
    ));

    addReplicationParameters(replicationConfig, args);

    execute(ozoneShell, args);
  }

  private void createKey(OzoneShell ozoneShell,
      String bucketName, String keyName,
      ReplicationConfig replicationConfig
  ) {
    List<String> args = new ArrayList<>(Arrays.asList(
        "key", "put",
        VOLUME_NAME + "/" + bucketName + "/" + keyName,
        testFile.getPath()));

    addReplicationParameters(replicationConfig, args);

    execute(ozoneShell, args);
  }

  private static void addReplicationParameters(ReplicationConfig replicationConfig, List<String> args) {
    if (replicationConfig != null) {
      args.addAll(Arrays.asList(
          "--type", replicationConfig.getReplicationType().name(),
          "--replication", replicationConfig.getReplication()
      ));
    }
  }

  private OzoneShell createSubject(ReplicationConfig clientReplication) {
    OzoneShell ozoneShell = new OzoneShell();

    ImmutableMap.Builder<String, String> config = new ImmutableMap.Builder<>();
    if (clientReplication != null) {
      config.put(OZONE_REPLICATION_TYPE, clientReplication.getReplicationType().name());
      config.put(OZONE_REPLICATION, clientReplication.getReplication());
    }
    config.put(OZONE_OM_ADDRESS_KEY, cluster().getConf().get(OZONE_OM_ADDRESS_KEY));
    ozoneShell.setConfigurationOverrides(config.build());

    return ozoneShell;
  }

  /**
   * Test the replication Order when different combinations of
   * bucket and key cli replication configs, bucket replication config
   * and server default replication configs are set.
   * Expected Order of preference is:
   * 1. Replication config params from CLI
   * 2. Replication configs from client configs
   * 2. Bucket Replication Config
   * 3. Server Default replication config
   */
  private void validateReplicationOrder(
      String bucketName, String keyName,
      ReplicationConfig serverReplication,
      ReplicationConfig clientReplication,
      ReplicationConfig bucketReplication,
      ReplicationConfig keyReplication
  ) throws Exception {
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketReplication, bucket.getReplicationConfig());

    OzoneKeyDetails key = bucket.getKey(keyName);
    final ReplicationConfig expected;
    if (keyReplication != null) {
      // if client sets replication config during key creation then
      // this replication config is used regardless of client configs or
      // bucket replication or server defaults.
      expected = keyReplication;
    } else if (clientReplication != null) {
      // if replication config are not passed as CLI params
      // during key creation and bucket replication config is set then
      // key uses bucket replication config
      // unless replication config is available in client configs.
      expected = clientReplication;
    } else if (bucketReplication != null) {
      // if replication config are not passed as CLI params
      // during key creation and bucket replication config is set then
      // key uses bucket replication config
      expected = bucketReplication;
    } else if (serverReplication != null) {
      // If replication is not set in client configs then key uses
      // server default replication config.
      expected = serverReplication;
    } else {
      expected = RatisReplicationConfig.getInstance(THREE);
    }

    ReplicationConfig actual = key.getReplicationConfig();

    assertEquals(expected, actual,
        () -> "key: " + describe(keyReplication)
            + " bucket: " + describe(bucketReplication)
            + " client: " + describe(clientReplication)
            + " server: " + describe(serverReplication)
    );
  }

  private static String describe(ReplicationConfig config) {
    return config != null
        ? config.getReplicationType() + "/" + config.getReplication()
        : "none";
  }

  private void updateReplicationInOM(ReplicationConfig replicationConfig) {
    if (replicationConfig != null) {
      updateReplicationInOM(replicationConfig.getReplicationType().name(), replicationConfig.getReplication());
    } else {
      updateReplicationInOM(null, null);
    }
  }

  private void updateReplicationInOM(@Nullable String type, @Nullable String params) {
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    TestHelper.setConfig(conf, OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY, type);
    TestHelper.setConfig(conf, OZONE_SERVER_DEFAULT_REPLICATION_KEY, params);
    cluster.getOzoneManager().setReplicationFromConfig();
  }

  @ParameterizedTest
  @MethodSource("replicationCombinations")
  void test(
      ReplicationConfig serverReplication,
      ReplicationConfig clientReplication,
      ReplicationConfig bucketReplication,
      ReplicationConfig keyReplication
  ) throws Exception {
    updateReplicationInOM(serverReplication);
    OzoneShell shell = createSubject(clientReplication);
    String bucketName = "bucket-" + UUID.randomUUID();
    String keyName = "key-" + UUID.randomUUID();
    createBucket(shell, bucketName, bucketReplication);
    createKey(shell, bucketName, keyName, keyReplication);
    validateReplicationOrder(bucketName, keyName,
        serverReplication, clientReplication, bucketReplication, keyReplication);
  }

  public List<Arguments> replicationCombinations() {
    List<Arguments> args = new ArrayList<>();
    for (ReplicationConfig serverReplication : REPLICATIONS) {
      for (ReplicationConfig clientReplication : REPLICATIONS) {
        for (ReplicationConfig bucketReplication : REPLICATIONS) {
          for (ReplicationConfig keyReplication : REPLICATIONS) {
            args.add(Arguments.of(serverReplication, clientReplication, bucketReplication, keyReplication));
          }
        }
      }
    }
    return args;
  }
}
