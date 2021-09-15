/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneOMHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;

import org.junit.AfterClass;
import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

/**
 * This class tests Ozone sh shell command with EC options.
 */
public class TestOzoneShellEC {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShellEC.class);

  /**
   * Set the timeout for every test.
   */
  @Rule public Timeout testTimeout = Timeout.seconds(300);

  private static File baseDir;
  private static File testFile;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static OzoneShell ozoneShell = null;
  private static String omServiceId;
  private static String clusterId;
  private static String scmId;
  private static int numOfOMs;

  /**
   * Create a MiniOzoneCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path =
        GenericTestUtils.getTempPath(TestOzoneShellHA.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    testFile = new File(path + OzoneConsts.OZONE_URI_DELIMITER + "testFile");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    ozoneShell = new OzoneShell();
    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newOMHABuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  private void execute(GenericCli shell, String[] args) {
    LOG.info("Executing OzoneShell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseRes) {
            throw ex;
          }
        };

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.parseWithHandlers(new RunLast(), exceptionHandler, argsWithHAConf);
  }

  /**
   * @return the leader OM's Node ID in the MiniOzoneHACluster.
   */
  private String getLeaderOMNodeId() {
    MiniOzoneOMHAClusterImpl haCluster = (MiniOzoneOMHAClusterImpl) cluster;
    OzoneManager omLeader = haCluster.getOMLeader();
    Assert
        .assertNotNull("There should be a leader OM at this point.", omLeader);
    return omLeader.getOMNodeId();
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

    res[indexOmServiceIds] =
        getSetConfStringFromConf(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey =
        ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert (omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] = getSetConfStringFromConf(ConfUtils
          .addKeySuffixes(OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId,
              omNodesArr[i]));
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

  @Test
  public void testCreateBucketWithECReplicationConfig() throws Exception {
    final String volumeName = "volume0";
    getVolume(volumeName);
    String[] args =
        new String[] {"bucket", "create", "/volume0/bucket0", "-rt", "EC", "-r",
            "3-2"};
    execute(ozoneShell, args);

    OzoneVolume volume =
        cluster.getClient().getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("myKey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testCreateBucketWithRatisReplicationConfig() throws Exception {
    final String volumeName = "volume1";
    getVolume(volumeName);
    String[] args = new String[] {"bucket", "create",
        "/volume1/bucket1", "-rt", "RATIS",
        "-r", "3"};
    execute(ozoneShell, args);

    OzoneVolume volume =
        cluster.getClient().getObjectStore().getVolume(volumeName);
    OzoneBucket bucket =
        volume.getBucket("bucket1");
    try (OzoneOutputStream out = bucket.createKey("myKey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof KeyOutputStream);
      Assert.assertFalse(out.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testCreateBucketWithECReplicationConfigWithoutReplicationParam() {
    getVolume("volume2");
    String[] args = new String[] {"bucket", "create",
        "/volume2/bucket2",
        "-rt", "EC"};
    try {
      execute(ozoneShell, args);
      Assert.fail("Must throw Exception when missing replication param");
    } catch (Exception e) {
      Assert.assertEquals(e.getCause().getMessage(),
          "Replication can't be null. Replication type passed was : EC");
    }
  }

  private void getVolume(String volumeName) {
    String[] args = new String[] {"volume", "create",
        "o3://" + omServiceId + "/" + volumeName};
    execute(ozoneShell, args);
  }
}
