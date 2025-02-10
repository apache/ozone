/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test the order of Replication config resolution.
 *
 */
@Timeout(100)
public class TestReplicationConfigPreference {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static String omServiceId;
  @TempDir
  private static java.nio.file.Path path;
  private static File testFile;
  private static final String DEFAULT_BUCKET = "default";
  private static final String RATIS_BUCKET = "ratis";
  private static final String EC_BUCKET = "ecbucket";
  private static final String DEFAULT_KEY = "defaultkey";
  private static final String RATIS_KEY = "ratiskey";
  private static final String EC_KEY = "eckey";
  private static String[] bucketList;
  private static String[] keyList;
  private static final ReplicationConfig RATIS_REPL_CONF =
      ReplicationConfig.fromProtoTypeAndFactor(RATIS, THREE);
  private static final ReplicationConfig EC_REPL_CONF = new ECReplicationConfig(
      3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB);

  protected static void startCluster(OzoneConfiguration ozoneConf)
      throws Exception {
    cluster = MiniOzoneCluster.newBuilder(ozoneConf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    omServiceId = cluster.getOzoneManager().getHostname() + ":" +
        cluster.getOzoneManager().getRpcPort();
  }

  @BeforeAll
  public static void init() throws Exception {
    testFile = new File(path + OZONE_URI_DELIMITER + "testFile");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();
    bucketList = new String[]{DEFAULT_BUCKET, RATIS_BUCKET, EC_BUCKET};
    keyList = new String[]{DEFAULT_KEY, RATIS_KEY, EC_KEY};
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected static void createAllKeys(OzoneShell ozoneShell,
                                      String volumeName, String bucketName) {
    ozoneShell.execute(new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + DEFAULT_KEY,
        testFile.getPath()});
    ozoneShell.execute(new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + RATIS_KEY,
        testFile.getPath(),
        "--type=" + RATIS_REPL_CONF.getReplicationType().name(),
        "--replication=" + RATIS_REPL_CONF.getReplication()});
    ozoneShell.execute(new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + EC_KEY,
        testFile.getPath(),
        "--type=" + EC_REPL_CONF.getReplicationType().name(),
        "--replication=" + EC_REPL_CONF.getReplication()});
  }

  protected static void createAllBucketsAndKeys(Map<String, String> clientConf,
                                                String volumeName) {
    OzoneShell ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    ozoneShell.execute(new String[] {"volume", "create", "o3://" + omServiceId
          + "/" + volumeName});
    ozoneShell.execute(new String[] {"bucket", "create", "o3://" + omServiceId
          + "/" + volumeName + "/" + DEFAULT_BUCKET});
    createAllKeys(ozoneShell, volumeName, DEFAULT_BUCKET);

    ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    ozoneShell.execute(new String[] {"bucket", "create", "o3://" + omServiceId
          + "/" + volumeName + "/" + RATIS_BUCKET,
        "--type=" + RATIS_REPL_CONF.getReplicationType().name(),
        "--replication=" + RATIS_REPL_CONF.getReplication()});
    createAllKeys(ozoneShell, volumeName, RATIS_BUCKET);

    ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    ozoneShell.execute(new String[] {"bucket", "create", "o3://" + omServiceId
          + "/" + volumeName + "/" + EC_BUCKET,
        "--type=" + EC_REPL_CONF.getReplicationType().name(),
        "--replication=" + EC_REPL_CONF.getReplication()});
    createAllKeys(ozoneShell, volumeName, EC_BUCKET);
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
  public void validateReplicationOrder(String volumeName,
                                       String clientConfigReplType,
                                       String clientConfigReplConfig,
                                       String serverDefaultReplType,
                                       String serverDefaultReplConfig) throws Exception {

    for (String b: bucketList) {
      OzoneBucket bucket = client.getObjectStore().getVolume(volumeName)
          .getBucket(b);
      if (b.equals(DEFAULT_BUCKET)) {
        assertNull(bucket.getReplicationConfig());
      } else {
        assertNotNull(bucket.getReplicationConfig());
      }

      for (String k: keyList) {
        OzoneKeyDetails key = bucket.getKey(k);
        if (b.equals(DEFAULT_BUCKET) && k.equals(DEFAULT_KEY)) {
          // if replication config are not passed as CLI params during bucket and
          // key creation then key uses client configs.
          // If replication is not set in client configs then key uses
          // server default replication config.
          if (clientConfigReplConfig != null) {
            assertEquals(clientConfigReplType, key.getReplicationConfig()
                .getReplicationType().name());
            assertEquals(clientConfigReplConfig, key.getReplicationConfig()
                .getReplication());
          } else {
            assertEquals(serverDefaultReplType, key.getReplicationConfig()
                .getReplicationType().name());
            assertEquals(serverDefaultReplConfig, key.getReplicationConfig()
                .getReplication());
          }
        } else if (k.equals(DEFAULT_KEY)) {
          // if replication config are not passed as CLI params
          // during key creation and bucket replication config is set then
          // key uses bucket replication config
          // unless replication config is available in client configs.
          if (clientConfigReplConfig != null) {
            assertEquals(clientConfigReplType, key.getReplicationConfig()
                .getReplicationType().name());
            assertEquals(clientConfigReplConfig, key.getReplicationConfig()
                .getReplication());
          } else {
            assertEquals(bucket.getReplicationConfig(),
                key.getReplicationConfig());
          }
        } else if (k.equals(RATIS_KEY)) {
          // if client sets replication config during key creation then
          // this replication config is used regardless of client configs or
          // bucket replication or server defaults.
          assertEquals(RATIS_REPL_CONF, key.getReplicationConfig());
        } else if (k.equals(EC_KEY)) {
          assertEquals(EC_REPL_CONF, key.getReplicationConfig());
        }
      }
    }
  }

  @Test
  public void testReplicationOrderDefaultRatisCluster() throws Exception {
    // Starting a cluster with server default replication type RATIS/THREE.
    shutdown();
    startCluster(new OzoneConfiguration());

    // Replication configs are not set in Client configuration.
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    createAllBucketsAndKeys(null, volumeName);
    validateReplicationOrder(volumeName, null, null,
        RATIS_REPL_CONF.getReplicationType().name(),
        RATIS_REPL_CONF.getReplication());

    // Replication configs are set in Client configuration.
    Map<String, String> clientConf = new HashMap<String, String>() {{
        put(OZONE_REPLICATION_TYPE, EC_REPL_CONF.getReplicationType().name());
        put(OZONE_REPLICATION, EC_REPL_CONF.getReplication());
      }};
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    createAllBucketsAndKeys(clientConf, volumeName);
    validateReplicationOrder(volumeName,
        EC_REPL_CONF.getReplicationType().name(),
        EC_REPL_CONF.getReplication(),
        RATIS_REPL_CONF.getReplicationType().name(),
        RATIS_REPL_CONF.getReplication());
  }

  @Test
  public void testReplicationOrderDefaultECCluster() throws Exception {
    // Starting a cluster with server default replication type EC.
    shutdown();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        EC_REPL_CONF.getReplicationType().name());
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        EC_REPL_CONF.getReplication());
    startCluster(conf);

    // Replication configs are not set in Client configuration.
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    createAllBucketsAndKeys(null, volumeName);
    validateReplicationOrder(volumeName, null, null,
        EC_REPL_CONF.getReplicationType().name(),
        EC_REPL_CONF.getReplication());

    // Replication configs are set in Client configuration.
    Map<String, String> clientConf = new HashMap<String, String>() {{
        put(OZONE_REPLICATION_TYPE, RATIS_REPL_CONF.getReplicationType().name());
        put(OZONE_REPLICATION, RATIS_REPL_CONF.getReplication());
      }};
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    createAllBucketsAndKeys(clientConf, volumeName);
    validateReplicationOrder(volumeName,
        RATIS_REPL_CONF.getReplicationType().name(),
        RATIS_REPL_CONF.getReplication(),
        EC_REPL_CONF.getReplicationType().name(),
        EC_REPL_CONF.getReplication());
  }
}
