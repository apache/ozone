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
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

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

  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String omServiceId;
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
  private static int numOfOMs = 3;
  private static final ReplicationConfig RATIS_REPL_CONF =
      ReplicationConfig.fromProtoTypeAndFactor(RATIS, THREE);
  private static final ReplicationConfig EC_REPL_CONF = new ECReplicationConfig(
      3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB);

  protected void startCluster()
      throws Exception {
    omServiceId = "om-service-test1";
    MiniOzoneHAClusterImpl.Builder builder =
        MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(5);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
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
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  /**
   * Helper function to get a String array to be fed into OzoneShell.
   * @param numOfArgs Additional number of arguments after the HA conf string,
   *                  this translates into the number of empty array elements
   *                  after the HA conf string.
   * @return String array.
   */
  private String[] getHASetConfStrings(int numOfArgs) {
    assert (numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert (omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(ConfUtils.addKeySuffixes(
              OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodesArr[i]));
    }

    return res;
  }

  /**
   * Helper function to create a new set of arguments that contains HA configs.
   * @param existingArgs Existing arguments to be fed into OzoneShell command.
   * @return String array.
   */
  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    for (int i = 0; i < existingArgs.length; i++) {
      res[indexCopyStart + i] = existingArgs[i];
    }
    return res;
  }

  private void execute(GenericCli shell, String[] args) {
    CommandLine cmd = shell.getCmd();

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.execute(argsWithHAConf);
  }

  protected void createAllKeys(OzoneShell ozoneShell,
      String volumeName, String bucketName) {
    execute(ozoneShell, new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + DEFAULT_KEY,
        testFile.getPath()});
    execute(ozoneShell, new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + RATIS_KEY,
        testFile.getPath(),
        "--type=" + RATIS_REPL_CONF.getReplicationType().name(),
        "--replication=" + RATIS_REPL_CONF.getReplication()});
    execute(ozoneShell, new String[] {"key", "put", "o3://" + omServiceId
          + "/" + volumeName + "/" + bucketName + "/" + EC_KEY,
        testFile.getPath(),
        "--type=" + EC_REPL_CONF.getReplicationType().name(),
        "--replication=" + EC_REPL_CONF.getReplication()});
  }

  protected void createAllBucketsAndKeys(Map<String, String> clientConf,
                                                String volumeName) {
    OzoneShell ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    execute(ozoneShell, new String[] {"volume", "create", "o3://" + omServiceId
          + "/" + volumeName});
    execute(ozoneShell, new String[] {"bucket", "create", "o3://" + omServiceId
          + "/" + volumeName + "/" + DEFAULT_BUCKET});
    createAllKeys(ozoneShell, volumeName, DEFAULT_BUCKET);

    ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    execute(ozoneShell, new String[] {"bucket", "create", "o3://" + omServiceId
          + "/" + volumeName + "/" + RATIS_BUCKET,
        "--type=" + RATIS_REPL_CONF.getReplicationType().name(),
        "--replication=" + RATIS_REPL_CONF.getReplication()});
    createAllKeys(ozoneShell, volumeName, RATIS_BUCKET);

    ozoneShell = new OzoneShell();
    if (clientConf != null) {
      ozoneShell.setConfigurationOverrides(clientConf);
    }
    execute(ozoneShell, new String[] {"bucket", "create", "o3://" + omServiceId
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
    conf = new OzoneConfiguration();
    startCluster();

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
    conf = new OzoneConfiguration();
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        EC_REPL_CONF.getReplicationType().name());
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        EC_REPL_CONF.getReplication());
    startCluster();

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
