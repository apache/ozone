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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.fs.ozone.OzoneFsShell;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.shell.s3.S3Shell;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.ozone.om.TrashPolicyOzone;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
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
 * This class tests Ozone sh shell command.
 * Inspired by TestS3Shell
 */
public class TestOzoneShellHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShellHA.class);

  private static final String DEFAULT_ENCODING = UTF_8.name();

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  private static File baseDir;
  private static File testFile;
  private static String testFilePathString;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private OzoneShell ozoneShell = null;
  private OzoneAdmin ozoneAdminShell = null;
  private S3Shell s3Shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

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
    OzoneConfiguration conf = new OzoneConfiguration();
    startCluster(conf);
  }

  protected static void startCluster(OzoneConfiguration conf) throws Exception {
    String path = GenericTestUtils.getTempPath(
        TestOzoneShellHA.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    testFilePathString = path + OZONE_URI_DELIMITER + "testFile";
    testFile = new File(testFilePathString);
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    final int numDNs = 5;
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(numDNs)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  @Before
  public void setup() throws UnsupportedEncodingException {
    ozoneShell = new OzoneShell();
    ozoneAdminShell = new OzoneAdmin();
    s3Shell = new S3Shell();
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @After
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
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
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(OzoneShell shell, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(shell, args);
    } else {
      try {
        execute(shell, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        if (!Strings.isNullOrEmpty(expectedError)) {
          Throwable exceptionToCheck = ex;
          if (exceptionToCheck.getCause() != null) {
            exceptionToCheck = exceptionToCheck.getCause();
          }
          Assert.assertTrue(
              String.format(
                  "Error of OzoneShell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
  }

  /**
   * @return the leader OM's Node ID in the MiniOzoneHACluster.
   */
  private String getLeaderOMNodeId() {
    MiniOzoneHAClusterImpl haCluster = (MiniOzoneHAClusterImpl) cluster;
    OzoneManager omLeader = haCluster.getOMLeader();
    Assert.assertNotNull("There should be a leader OM at this point.",
        omLeader);
    return omLeader.getOMNodeId();
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, cluster.getConf().get(key));
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
    String omNodesVal = cluster.getConf().get(omNodesKey);
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

  /**
   * Helper function to generate keys for testing shell command of keys.
   */
  private void generateKeys(String volumeName, String bucketName,
                            String bucketLayout) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    args = (Strings.isNullOrEmpty(bucketLayout)) ?
            new String[] {"bucket", "create", "o3://" + omServiceId +
                    volumeName + bucketName } :
            new String[] {"bucket", "create", "o3://" + omServiceId +
                    volumeName + bucketName, "--layout", bucketLayout};
    execute(ozoneShell, args);

    String keyName = volumeName + bucketName + OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 100; i++) {
      args = new String[] {
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
  }

  /**
   * Helper function to get nums of keys from info of listing command.
   */
  private int getNumOfKeys() throws UnsupportedEncodingException {
    return out.toString(DEFAULT_ENCODING).split("key").length - 1;
  }

  /**
   * Helper function to generate buckets for testing shell command of buckets.
   */
  private void generateBuckets(String volumeName, int numOfBuckets) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    String bucketName =
        volumeName + OZONE_URI_DELIMITER + "testbucket";
    for (int i = 0; i < numOfBuckets; i++) {
      args = new String[] {
          "bucket", "create", "o3://" + omServiceId + bucketName + i};
      execute(ozoneShell, args);
    }
  }

  /**
   * Helper function to get nums of buckets from info of listing command.
   */
  private int getNumOfBuckets(String bucketPrefix)
      throws UnsupportedEncodingException {
    return out.toString(DEFAULT_ENCODING).split(bucketPrefix).length - 1;
  }

  /**
   * Parse output into ArrayList with Gson.
   * @return ArrayList
   */
  private ArrayList<LinkedTreeMap<String, String>> parseOutputIntoArrayList()
      throws UnsupportedEncodingException {
    return new Gson().fromJson(out.toString(DEFAULT_ENCODING), ArrayList.class);
  }

  @Test
  public void testRATISTypeECReplication() {
    String[] args = new String[]{"bucket", "create", "/vol/bucket",
        "--type=" + ReplicationType.RATIS, "--replication=rs-3-2-1024k"};
    Throwable t = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, args));
    Throwable c = t.getCause();
    assertTrue(c instanceof IllegalArgumentException);
    assertEquals("rs-3-2-1024k is not supported for " +
            ReplicationType.RATIS + " replication type", c.getMessage());
  }

  /**
   * Tests ozone sh command URI parsing with volume and bucket create commands.
   */
  @Test
  public void testOzoneShCmdURIs() {


    // Get leader OM node RPC address from ozone.om.address.omServiceId.omNode
    String omLeaderNodeId = getLeaderOMNodeId();
    String omLeaderNodeAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omLeaderNodeId);
    String omLeaderNodeAddr = cluster.getConf().get(omLeaderNodeAddrKey);
    String omLeaderNodeAddrWithoutPort = omLeaderNodeAddr.split(":")[0];

    // Test case 2: ozone sh volume create o3://om1/volume2
    // Expectation: Success.
    // Note: For now it seems OzoneShell is only trying the default port 9862
    // instead of using the port defined in ozone.om.address (as ozone fs does).
    // So the test will fail before this behavior is fixed.
    // TODO: Fix this behavior, then uncomment the execute() below.
    String setOmAddress = "--set=" + OMConfigKeys.OZONE_OM_ADDRESS_KEY + "="
        + omLeaderNodeAddr;
    String[] args = new String[] {setOmAddress, "volume", "create",
        "o3://" + omLeaderNodeAddrWithoutPort + "/volume2"};
    execute(ozoneShell, args);

    // Test case 3: ozone sh volume create o3://om1:port/volume3
    // Expectation: Success.
    args = new String[] {
        "volume", "create", "o3://" + omLeaderNodeAddr + "/volume3"};
    execute(ozoneShell, args);

    // Test case 4: ozone sh volume create o3://id1/volume
    // Expectation: Success.
    args = new String[] {"volume", "create", "o3://" + omServiceId + "/volume"};
    execute(ozoneShell, args);

    // Test case 5: ozone sh volume create o3://id1:port/volume
    // Expectation: Failure.
    args = new String[] {"volume", "create",
        "o3://" + omServiceId + ":9862" + "/volume"};
    executeWithError(ozoneShell, args, "does not use port information");

    // Test case 6: ozone sh bucket create /volume/bucket
    // Expectation: Success.
    args = new String[] {"bucket", "create", "/volume/bucket-one"};
    execute(ozoneShell, args);

    // Test case 7: ozone sh bucket create o3://om1/volume/bucket
    // Expectation: Success.
    args = new String[] {
        "bucket", "create", "o3://" + omServiceId + "/volume/bucket-two"};
    execute(ozoneShell, args);
  }

  /**
   * Test ozone shell list command.
   */
  @Test
  public void testOzoneShCmdList() throws UnsupportedEncodingException {
    // Part of listing keys test.
    generateKeys("/volume4", "/bucket", "");
    final String destinationBucket = "o3://" + omServiceId + "/volume4/bucket";

    // Test case 1: test listing keys
    // ozone sh key list /volume4/bucket
    // Expectation: Get list including all keys.
    String[] args = new String[] {"key", "list", destinationBucket};
    out.reset();
    execute(ozoneShell, args);
    Assert.assertEquals(100, getNumOfKeys());

    // Test case 2: test listing keys for setting --start with last key.
    // ozone sh key list --start=key99 /volume4/bucket
    // Expectation: Get empty list.
    final String startKey = "--start=key99";
    args = new String[] {"key", "list", startKey, destinationBucket};
    out.reset();
    execute(ozoneShell, args);
    // Expect empty JSON array
    Assert.assertEquals(0, parseOutputIntoArrayList().size());
    Assert.assertEquals(0, getNumOfKeys());

    // Part of listing buckets test.
    generateBuckets("/volume5", 100);
    final String destinationVolume = "o3://" + omServiceId + "/volume5";

    // Test case 1: test listing buckets.
    // ozone sh bucket list /volume5
    // Expectation: Get list including all buckets.
    args = new String[] {"bucket", "list", destinationVolume};
    out.reset();
    execute(ozoneShell, args);
    Assert.assertEquals(100, getNumOfBuckets("testbucket"));

    // Test case 2: test listing buckets for setting --start with last bucket.
    // ozone sh bucket list /volume5 --start=bucket99 /volume5
    // Expectation: Get empty list.
    final String startBucket = "--start=testbucket99";
    out.reset();
    args = new String[] {"bucket", "list", startBucket, destinationVolume};
    execute(ozoneShell, args);
    // Expect empty JSON array
    Assert.assertEquals(0, parseOutputIntoArrayList().size());
    Assert.assertEquals(0, getNumOfBuckets("testbucket"));
  }

  /**
   * Test ozone admin list command.
   */
  @Test
  public void testOzoneAdminCmdList() throws UnsupportedEncodingException {
    // Part of listing keys test.
    generateKeys("/volume6", "/bucket", "");
    // Test case 1: list OPEN container
    String state = "--state=OPEN";
    String[] args = new String[] {"container", "list", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort(),
        state};
    execute(ozoneAdminShell, args);

    // Test case 2: list CLOSED container
    state = "--state=CLOSED";
    args = new String[] {"container", "list", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort(),
        state};
    execute(ozoneAdminShell, args);

    // Test case 3: list THREE replica container
    String factor = "--replication=THREE";
    args = new String[] {"container", "list", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort(),
        factor, "--type=RATIS"};
    execute(ozoneAdminShell, args);

    // Test case 4: list ONE replica container
    factor = "--replication=ONE";
    args = new String[] {"container", "list", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort(),
        factor, "--type=RATIS"};
    execute(ozoneAdminShell, args);
  }

  /**
   * Helper function to retrieve Ozone client configuration for trash testing.
   * @param hostPrefix Scheme + Authority. e.g. ofs://om-service-test1
   * @param configuration Server config to generate client config from.
   * @return Config added with fs.ofs.impl, fs.defaultFS and fs.trash.interval.
   */
  private OzoneConfiguration getClientConfForOFS(
      String hostPrefix, OzoneConfiguration configuration) {

    OzoneConfiguration clientConf = new OzoneConfiguration(configuration);
    // fs.ofs.impl should be loaded from META-INF, no need to explicitly set it
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);
    clientConf.setInt(FS_TRASH_INTERVAL_KEY, 60);
    return clientConf;
  }

  /**
   * Helper function to retrieve Ozone client configuration for ozone
   * trash testing with TrashPolicyOzone.
   * @param hostPrefix Scheme + Authority. e.g. ofs://om-service-test1
   * @param configuration Server config to generate client config from.
   * @return Config ofs configuration added with fs.trash.classname
   * = TrashPolicyOzone.
   */
  private OzoneConfiguration getClientConfForOzoneTrashPolicy(
          String hostPrefix, OzoneConfiguration configuration) {
    OzoneConfiguration clientConf =
            getClientConfForOFS(hostPrefix, configuration);
    clientConf.setClass("fs.trash.classname", TrashPolicyOzone.class,
            TrashPolicy.class);
    return clientConf;
  }

  @Test
  public void testDeleteToTrashOrSkipTrash() throws Exception {
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    OzoneConfiguration clientConf =
        getClientConfForOFS(hostPrefix, cluster.getConf());
    OzoneFsShell shell = new OzoneFsShell(clientConf);
    FileSystem fs = FileSystem.get(clientConf);
    String ofsPrefix = hostPrefix + "/volumed2t/bucket1";
    String dir1 = "/dir1";
    final String strDir1 = ofsPrefix + dir1;
    // Note: CURRENT is also privately defined in TrashPolicyDefault
    final Path trashCurrent = new Path("Current");

    final String strKey1 = strDir1 + "/key1";
    final Path pathKey1 = new Path(strKey1);
    final Path trashPathKey1 = Path.mergePaths(
        new Path(new OFSPath(strKey1, clientConf).getTrashRoot(),
            trashCurrent), new Path(dir1, "key1"));

    final String strKey2 = strDir1 + "/key2";
    final Path pathKey2 = new Path(strKey2);
    final Path trashPathKey2 = Path.mergePaths(
        new Path(new OFSPath(strKey2, clientConf).getTrashRoot(),
            trashCurrent), new Path(dir1, "key2"));

    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p", strDir1});
      Assert.assertEquals(0, res);

      // Check delete to trash behavior
      res = ToolRunner.run(shell, new String[]{"-touch", strKey1});
      Assert.assertEquals(0, res);
      // Verify key1 creation
      FileStatus statusPathKey1 = fs.getFileStatus(pathKey1);

      FileChecksum previousFileChecksum = fs.getFileChecksum(pathKey1);

      Assert.assertEquals(strKey1, statusPathKey1.getPath().toString());
      // rm without -skipTrash. since trash interval > 0, should moved to trash
      res = ToolRunner.run(shell, new String[]{"-rm", strKey1});
      Assert.assertEquals(0, res);

      FileChecksum afterFileChecksum = fs.getFileChecksum(trashPathKey1);

      // Verify that the file is moved to the correct trash location
      FileStatus statusTrashPathKey1 = fs.getFileStatus(trashPathKey1);
      // It'd be more meaningful if we actually write some content to the file
      Assert.assertEquals(
          statusPathKey1.getLen(), statusTrashPathKey1.getLen());
      Assert.assertEquals(previousFileChecksum, afterFileChecksum);

      // Check delete skip trash behavior
      res = ToolRunner.run(shell, new String[]{"-touch", strKey2});
      Assert.assertEquals(0, res);
      // Verify key2 creation
      FileStatus statusPathKey2 = fs.getFileStatus(pathKey2);
      Assert.assertEquals(strKey2, statusPathKey2.getPath().toString());
      // rm with -skipTrash
      res = ToolRunner.run(shell, new String[]{"-rm", "-skipTrash", strKey2});
      Assert.assertEquals(0, res);
      // Verify that the file is NOT moved to the trash location
      try {
        fs.getFileStatus(trashPathKey2);
        Assert.fail("getFileStatus on non-existent should throw.");
      } catch (FileNotFoundException ignored) {
      }
    } finally {
      shell.close();
      fs.close();
    }
  }

  @Test
  public void testLinkBucketOrphan() throws Exception {
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    OzoneConfiguration clientConf =
        getClientConfForOFS(hostPrefix, cluster.getConf());
    OzoneFsShell shell = new OzoneFsShell(clientConf);
    FileSystem fs = FileSystem.get(clientConf);

    int res;
    try {
      // Test orphan link bucket when source volume removed
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/vol1/bucket1"});
      Assert.assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/linkvol"});
      Assert.assertEquals(0, res);

      String[] args =
          new String[] {"bucket", "link", "/vol1/bucket1",
              "/linkvol/linkbuck"};
      execute(ozoneShell, args);
      
      res = ToolRunner.run(shell, new String[]{"-rm", "-R", "-f",
          "-skipTrash", hostPrefix + "/vol1"});
      Assert.assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-ls", "-R",
          hostPrefix + "/linkvol"});
      Assert.assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-rm", "-R", "-f",
          "-skipTrash", hostPrefix + "/linkvol"});
      Assert.assertEquals(0, res);

      // Test orphan link bucket when only source bucket removed
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/vol1/bucket1"});
      Assert.assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/linkvol"});
      Assert.assertEquals(0, res);

      args = new String[]{"bucket", "link", "/vol1/bucket1",
          "/linkvol/linkbuck"};
      execute(ozoneShell, args);

      res = ToolRunner.run(shell, new String[]{"-rm", "-R", "-f",
          "-skipTrash", hostPrefix + "/vol1/bucket1"});
      Assert.assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-rm", "-R", "-f",
          "-skipTrash", hostPrefix + "/linkvol"});
      Assert.assertEquals(0, res);
    } finally {
      shell.close();
      fs.close();
    }
  }

  @Test
  public void testDeleteTrashNoSkipTrash() throws Exception {

    // Test delete from Trash directory removes item from filesystem

    // setup configuration to use TrashPolicyOzone
    // (default is TrashPolicyDefault)
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    OzoneConfiguration clientConf =
            getClientConfForOzoneTrashPolicy(hostPrefix, cluster.getConf());
    OzoneFsShell shell = new OzoneFsShell(clientConf);

    int res;

    // create volume: vol1 with bucket: bucket1
    final String testVolBucket = "/vol1/bucket1";
    String keyName = "/key1";
    final String testKey = testVolBucket + keyName;

    final String[] volBucketArgs = new String[] {"-mkdir", "-p", testVolBucket};
    final String[] keyArgs = new String[] {"-touch", testKey};
    final String[] listArgs = new String[] {"key", "list", testVolBucket};

    LOG.info("Executing testDeleteTrashNoSkipTrash: FsShell with args {}",
            Arrays.asList(volBucketArgs));
    res = ToolRunner.run(shell, volBucketArgs);
    Assert.assertEquals(0, res);

    // create key: key1 belonging to bucket1
    res = ToolRunner.run(shell, keyArgs);
    Assert.assertEquals(0, res);

    // check directory listing for bucket1 contains 1 key
    out.reset();
    execute(ozoneShell, listArgs);
    Assert.assertEquals(1, getNumOfKeys());

    // Test deleting items in trash are discarded (removed from filesystem)
    // 1.) remove key1 from bucket1 with fs shell rm command
    // 2.) on rm, item is placed in Trash
    // 3.) remove Trash directory and all contents, 
    //     check directory listing = 0 items

    final String[] rmKeyArgs = new String[] {"-rm", "-R", testKey};
    final String[] rmTrashArgs = new String[] {"-rm", "-R",
                                               testVolBucket + "/.Trash"};
    final Path trashPathKey1 = Path.mergePaths(new Path(
            new OFSPath(testKey, clientConf).getTrashRoot(),
            new Path("Current")), new Path(keyName));
    FileSystem fs = FileSystem.get(clientConf);

    try {
      // on delete key, item is placed in trash
      LOG.info("Executing testDeleteTrashNoSkipTrash: FsShell with args {}",
              Arrays.asList(rmKeyArgs));
      res = ToolRunner.run(shell, rmKeyArgs);
      Assert.assertEquals(0, res);

      LOG.info("Executing testDeleteTrashNoSkipTrash: key1 deleted moved to"
              + " Trash: " + trashPathKey1.toString());
      fs.getFileStatus(trashPathKey1);

      LOG.info("Executing testDeleteTrashNoSkipTrash: deleting trash FsShell "
              + "with args{}: ", Arrays.asList(rmTrashArgs));
      res = ToolRunner.run(shell, rmTrashArgs);
      Assert.assertEquals(0, res);

      out.reset();
      // once trash is is removed, trash should be deleted from filesystem
      execute(ozoneShell, listArgs);
      Assert.assertEquals(0, getNumOfKeys());

    } finally {
      shell.close();
      fs.close();
    }

  }


  @Test
  @SuppressWarnings("methodlength")
  public void testShQuota() throws Exception {
    ObjectStore objectStore = client.getObjectStore();

    // Test create with no quota
    String[] args = new String[]{"volume", "create", "vol"};
    execute(ozoneShell, args);
    assertEquals(-1, objectStore.getVolume("vol").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol").getQuotaInNamespace());
    out.reset();

    args = new String[]{"bucket", "create", "vol/buck"};
    execute(ozoneShell, args);
    assertEquals(-1,
        objectStore.getVolume("vol").getBucket("buck").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol").getBucket("buck")
            .getQuotaInNamespace());

    // Test --quota option.
    args = new String[]{"volume", "create", "vol1", "--quota", "100B"};
    execute(ozoneShell, args);
    assertEquals(100, objectStore.getVolume("vol1").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol1").getQuotaInNamespace());
    out.reset();

    args =
        new String[]{"bucket", "create", "vol1/buck1", "--quota", "10B"};
    execute(ozoneShell, args);
    assertEquals(10,
        objectStore.getVolume("vol1").getBucket("buck1").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol1").getBucket("buck1")
            .getQuotaInNamespace());

    // Test --space-quota option.
    args = new String[]{"volume", "create", "vol2", "--space-quota",
        "100B"};
    execute(ozoneShell, args);
    assertEquals(100, objectStore.getVolume("vol2").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol2").getQuotaInNamespace());
    out.reset();

    args = new String[]{"bucket", "create", "vol2/buck2", "--space-quota",
        "10B"};
    execute(ozoneShell, args);
    assertEquals(10,
        objectStore.getVolume("vol2").getBucket("buck2").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol2").getBucket("buck2")
            .getQuotaInNamespace());

    // Test --namespace-quota option.
    args =
        new String[]{"volume", "create", "vol3", "--namespace-quota", "100"};
    execute(ozoneShell, args);
    assertEquals(-1, objectStore.getVolume("vol3").getQuotaInBytes());
    assertEquals(100,
        objectStore.getVolume("vol3").getQuotaInNamespace());
    out.reset();

    args = new String[]{"bucket", "create", "vol3/buck3",
        "--namespace-quota", "10"};
    execute(ozoneShell, args);
    assertEquals(-1,
        objectStore.getVolume("vol3").getBucket("buck3").getQuotaInBytes());
    assertEquals(10,
        objectStore.getVolume("vol3").getBucket("buck3")
            .getQuotaInNamespace());

    // Test both --space-quota and --namespace-quota option.
    args = new String[]{"volume", "create", "vol4", "--space-quota",
        "100B", "--namespace-quota", "100"};
    execute(ozoneShell, args);
    assertEquals(100, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(100,
        objectStore.getVolume("vol4").getQuotaInNamespace());
    out.reset();

    args = new String[]{"bucket", "create", "vol4/buck4",
        "--space-quota", "10B", "--namespace-quota", "10"};
    execute(ozoneShell, args);
    assertEquals(10,
        objectStore.getVolume("vol4").getBucket("buck4").getQuotaInBytes());
    assertEquals(10,
        objectStore.getVolume("vol4").getBucket("buck4")
            .getQuotaInNamespace());

    // Test clrquota option.
    args = new String[]{"volume", "clrquota", "vol4"};
    executeWithError(ozoneShell, args, "At least one of the quota clear" +
        " flag is required");
    out.reset();
    
    args = new String[]{"volume", "clrquota", "vol4", "--space-quota",
        "--namespace-quota"};
    execute(ozoneShell, args);
    assertEquals(-1, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol4").getQuotaInNamespace());
    out.reset();

    args = new String[]{"bucket", "clrquota", "vol4/buck4"};
    executeWithError(ozoneShell, args, "At least one of the quota clear" +
        " flag is required");
    out.reset();

    args = new String[]{"bucket", "clrquota", "vol4/buck4",
        "--space-quota", "--namespace-quota"};
    execute(ozoneShell, args);
    assertEquals(-1,
        objectStore.getVolume("vol4").getBucket("buck4").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol4").getBucket("buck4")
            .getQuotaInNamespace());
    out.reset();

    // Test set volume quota to 0.
    String[] volumeArgs1 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "0GB"};
    LambdaTestUtils.intercept(ExecutionException.class,
        "Invalid values for space quota",
        () -> execute(ozoneShell, volumeArgs1));
    out.reset();

    String[] volumeArgs2 = new String[]{"volume", "setquota", "vol4",
        "--namespace-quota", "0"};
    LambdaTestUtils.intercept(ExecutionException.class,
        "Invalid values for namespace quota",
        () -> execute(ozoneShell, volumeArgs2));
    out.reset();

    // Test set bucket quota to 0.
    String[] bucketArgs1 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "0GB"};
    LambdaTestUtils.intercept(ExecutionException.class,
        "Invalid values for space quota",
        () -> execute(ozoneShell, bucketArgs1));
    out.reset();

    String[] bucketArgs2 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--namespace-quota", "0"};
    LambdaTestUtils.intercept(ExecutionException.class,
        "Invalid values for namespace quota",
        () -> execute(ozoneShell, bucketArgs2));
    out.reset();

    // Test set bucket spaceQuota or nameSpaceQuota to normal value.
    String[] bucketArgs3 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "1000B"};
    execute(ozoneShell, bucketArgs3);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(-1, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    String[] bucketArgs4 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--namespace-quota", "100"};
    execute(ozoneShell, bucketArgs4);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(100, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    // test whether supports default quota unit as bytes.
    String[] bucketArgs5 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "500"};
    execute(ozoneShell, bucketArgs5);
    out.reset();
    assertEquals(500, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(100, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    // Test set volume quota without quota flag
    String[] bucketArgs6 = new String[]{"bucket", "setquota", "vol4/buck4"};
    executeWithError(ozoneShell, bucketArgs6,
        "At least one of the quota set flag is required");
    out.reset();

    // Test set volume spaceQuota or nameSpaceQuota to normal value.
    String[] volumeArgs3 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "1000B"};
    execute(ozoneShell, volumeArgs3);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol4").getQuotaInNamespace());

    String[] volumeArgs4 = new String[]{"volume", "setquota", "vol4",
        "--namespace-quota", "100"};
    execute(ozoneShell, volumeArgs4);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(100,
        objectStore.getVolume("vol4").getQuotaInNamespace());

    // Test set volume quota without quota flag
    String[] volumeArgs5 = new String[]{"volume", "setquota", "vol4"};
    executeWithError(ozoneShell, volumeArgs5,
        "At least one of the quota set flag is required");
    out.reset();
    
    objectStore.getVolume("vol").deleteBucket("buck");
    objectStore.deleteVolume("vol");
    objectStore.getVolume("vol1").deleteBucket("buck1");
    objectStore.deleteVolume("vol1");
    objectStore.getVolume("vol2").deleteBucket("buck2");
    objectStore.deleteVolume("vol2");
    objectStore.getVolume("vol3").deleteBucket("buck3");
    objectStore.deleteVolume("vol3");
    objectStore.getVolume("vol4").deleteBucket("buck4");
    objectStore.deleteVolume("vol4");
  }

  @Test
  public void testCreateBucketWithECReplicationConfig() throws Exception {
    final String volumeName = "volume100";
    getVolume(volumeName);
    String[] args =
        new String[] {"bucket", "create", "/volume100/bucket0", "-t", "EC",
            "-r", "rs-3-2-1024k"};
    execute(ozoneShell, args);

    OzoneVolume volume =
        client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("myKey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testPutKeyOnBucketWithECReplicationConfig() throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();
    getVolume(volumeName);
    String bucketPath =
        Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String[] args =
        new String[] {"bucket", "create", bucketPath, "-t", "EC", "-r",
            "rs-3-2-1024k"};
    execute(ozoneShell, args);

    args = new String[] {"key", "put", bucketPath + Path.SEPARATOR + keyName,
        testFilePathString};
    execute(ozoneShell, args);

    OzoneKeyDetails key =
        client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName).getKey(keyName);
    assertEquals(HddsProtos.ReplicationType.EC,
        key.getReplicationConfig().getReplicationType());
  }

  @Test
  public void testCreateBucketWithRatisReplicationConfig() throws Exception {
    final String volumeName = "volume101";
    getVolume(volumeName);
    String[] args =
        new String[] {"bucket", "create", "/volume101/bucket1", "-t", "RATIS",
            "-r", "3"};
    execute(ozoneShell, args);

    OzoneVolume volume =
        client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket("bucket1");
    try (OzoneOutputStream out = bucket.createKey("myKey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof KeyOutputStream);
      Assert.assertFalse(out.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testSetECReplicationConfigOnBucket() throws Exception {
    final String volumeName = "volume110";
    getVolume(volumeName);
    String bucketPath = "/volume110/bucket0";
    String[] args = new String[] {"bucket", "create", bucketPath};
    execute(ozoneShell, args);

    OzoneVolume volume =
        client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("myNonECKey", 1024)) {
      Assert.assertFalse(out.getOutputStream().getClass().getName()
          .equals(ECKeyOutputStream.class.getName()));
    }

    args = new String[] {"bucket", "set-replication-config", bucketPath, "-t",
        "EC", "-r", "rs-3-2-1024k"};
    execute(ozoneShell, args);
    bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("newECKey", 1024)) {
      Assert.assertTrue(out.getOutputStream().getClass().getName()
          .equals(ECKeyOutputStream.class.getName()));
    }

    args = new String[] {"bucket", "set-replication-config", bucketPath, "-t",
        "RATIS", "-r", "THREE"};
    execute(ozoneShell, args);
    bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("newNonECKey", 1024)) {
      Assert.assertFalse(out.getOutputStream().getClass().getName()
          .equals(ECKeyOutputStream.class.getName()));
    }
  }

  @Test
  public void testCreateBucketWithECReplicationConfigWithoutReplicationParam() {
    getVolume("volume102");
    String[] args =
        new String[] {"bucket", "create", "/volume102/bucket2", "-t", "EC"};
    try {
      execute(ozoneShell, args);
      Assert.fail("Must throw Exception when missing replication param");
    } catch (Exception e) {
      Assert.assertEquals(
          "Replication can't be null. Replication type passed was : EC",
          e.getCause().getMessage());
    }
  }


  @Test
  public void testKeyDeleteOrSkipTrashWhenTrashEnableFSO()
      throws IOException {
    // Create 100 keys
    generateKeys("/volumefso1", "/bucket1",
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());

    // Enable trash
    String trashConfKey = generateSetConfString(
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, "1");
    String[] args =
        new String[] {trashConfKey, "key", "delete",
            "/volumefso1/bucket1/key4"};

    // Delete one key from FSO bucket
    execute(ozoneShell, args);

    // Get key list in .Trash path
    String prefixKey = "--prefix=.Trash";
    args = new String[] {"key", "list", prefixKey, "o3://" +
          omServiceId + "/volumefso1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);

    // One key should be present in .Trash
    Assert.assertEquals(1, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" + omServiceId +
          "/volumefso1/bucket1/", "-l ", "110"};
    out.reset();
    execute(ozoneShell, args);

    // Total number of keys still 100.
    Assert.assertEquals(100, getNumOfKeys());

    // Skip Trash
    args = new String[] {trashConfKey, "key", "delete",
        "/volumefso1/bucket1/key5", "--skipTrash"};
    execute(ozoneShell, args);

    // .Trash should still contain 1 key
    prefixKey = "--prefix=.Trash";
    args = new String[] {"key", "list", prefixKey, "o3://" +
          omServiceId + "/volumefso1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);
    Assert.assertEquals(1, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" + omServiceId +
          "/volumefso1/bucket1/", "-l ", "110"};
    out.reset();
    execute(ozoneShell, args);
    // Total number of keys now will be 99 as
    // 1 key deleted without trash
    Assert.assertEquals(99, getNumOfKeys());

    final String username =
        UserGroupInformation.getCurrentUser().getShortUserName();
    Path trashRoot = new Path(OZONE_URI_DELIMITER, TRASH_PREFIX);
    Path userTrash = new Path(trashRoot, username);
    Path current = new Path("Current");
    Path userTrashCurrent = new Path(userTrash, current);

    // Try to delete from trash path
    args = new String[] {trashConfKey, "key", "delete",
        "/volumefso1/bucket1/" + userTrashCurrent.toUri().getPath()
          + "/key4"};

    out.reset();
    execute(ozoneShell, args);

    args = new String[] {"key", "list", "o3://" + omServiceId +
          "/volumefso1/bucket1/", "-l ", "110"};
    out.reset();
    execute(ozoneShell, args);

    // Total number of keys still remain 99 as
    // delete from trash not allowed without --skipTrash
    Assert.assertEquals(99, getNumOfKeys());

    // Now try to delete from trash path with --skipTrash option
    args = new String[] {trashConfKey, "key", "delete",
        "/volumefso1/bucket1/" + userTrashCurrent.toUri().getPath()
          + "/key4", "--skipTrash"};
    out.reset();
    execute(ozoneShell, args);

    args = new String[] {"key", "list", "o3://" + omServiceId +
          "/volumefso1/bucket1/", "-l ", "110"};
    out.reset();
    execute(ozoneShell, args);

    // Total number of keys now will be 98 as
    // 1 key deleted without trash and 1 from the trash path
    Assert.assertEquals(98, getNumOfKeys());
  }

  @Test
  public void testKeyDeleteWhenTrashDisableFSO()
      throws UnsupportedEncodingException {
    // Create 100 keys
    generateKeys("/volumefso2", "/bucket2",
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());
    // Disable trash
    String trashConfKey = generateSetConfString(
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, "0");
    String[] args =
            new String[] {trashConfKey, "key",
                "delete", "/volumefso2/bucket2/key4"};

    execute(ozoneShell, args);

    // Check in .Trash path number of keys
    final String prefixKey = "--prefix=.Trash";
    args = new String[] {"key", "list", prefixKey,
        "o3://" + omServiceId + "/volumefso2/bucket2/"};
    out.reset();
    execute(ozoneShell, args);

    // No key should be present in .Trash
    Assert.assertEquals(0, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" +
          omServiceId + "/volumefso2/bucket2/"};
    out.reset();
    execute(ozoneShell, args);

    // Number of keys remain as 99
    Assert.assertEquals(99, getNumOfKeys());
  }

  @Test
  public void testKeyDeleteWhenTrashEnableOBS()
      throws UnsupportedEncodingException {
    generateKeys("/volumeobs1", "/bucket1",
        BucketLayout.OBJECT_STORE.toString());

    String trashConfKey = generateSetConfString(
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, "1");
    String[] args =
            new String[] {trashConfKey, "key",
                "delete", "/volumeobs1/bucket1/key4"};
    execute(ozoneShell, args);

    final String prefixKey = "--prefix=.Trash";
    args = new String[] {"key", "list", prefixKey, "o3://" +
          omServiceId + "/volumeobs1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);
    Assert.assertEquals(0, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" +
          omServiceId + "/volumeobs1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);

    Assert.assertEquals(99, getNumOfKeys());
  }


  private void getVolume(String volumeName) {
    String[] args = new String[] {"volume", "create",
        "o3://" + omServiceId + "/" + volumeName};
    execute(ozoneShell, args);
  }

  public void testListVolumeBucketKeyShouldPrintValidJsonArray()
      throws UnsupportedEncodingException {

    final List<String> testVolumes =
        Arrays.asList("jsontest-vol1", "jsontest-vol2", "jsontest-vol3");
    final List<String> testBuckets =
        Arrays.asList("v1-bucket1", "v1-bucket2", "v1-bucket3");
    final List<String> testKeys = Arrays.asList("key1", "key2", "key3");

    final String firstVolumePrefix = testVolumes.get(0) + OZONE_URI_DELIMITER;
    final String keyPathPrefix = firstVolumePrefix +
        testBuckets.get(0) + OZONE_URI_DELIMITER;

    // Create test volumes, buckets and keys
    testVolumes.forEach(vol -> execute(ozoneShell, new String[] {
        "volume", "create", vol}));
    testBuckets.forEach(bucket -> execute(ozoneShell, new String[] {
        "bucket", "create", firstVolumePrefix + bucket}));
    testKeys.forEach(key -> execute(ozoneShell, new String[] {
        "key", "put", keyPathPrefix + key, testFilePathString}));

    // ozone sh volume list
    out.reset();
    execute(ozoneShell, new String[] {"volume", "list"});

    // Expect valid JSON array
    final ArrayList<LinkedTreeMap<String, String>> volumeListOut =
        parseOutputIntoArrayList();
    // Can include s3v and volumes from other test cases that aren't cleaned up,
    //  hence >= instead of equals.
    Assert.assertTrue(volumeListOut.size() >= testVolumes.size());
    final HashSet<String> volumeSet = new HashSet<>(testVolumes);
    volumeListOut.forEach(map -> volumeSet.remove(map.get("name")));
    // Should have found all the volumes created for this test
    Assert.assertEquals(0, volumeSet.size());

    // ozone sh bucket list jsontest-vol1
    out.reset();
    execute(ozoneShell, new String[] {"bucket", "list", firstVolumePrefix});

    // Expect valid JSON array as well
    final ArrayList<LinkedTreeMap<String, String>> bucketListOut =
        parseOutputIntoArrayList();
    Assert.assertEquals(testBuckets.size(), bucketListOut.size());
    final HashSet<String> bucketSet = new HashSet<>(testBuckets);
    bucketListOut.forEach(map -> bucketSet.remove(map.get("name")));
    // Should have found all the buckets created for this test
    Assert.assertEquals(0, bucketSet.size());

    // ozone sh key list jsontest-vol1/v1-bucket1
    out.reset();
    execute(ozoneShell, new String[] {"key", "list", keyPathPrefix});

    // Expect valid JSON array as well
    final ArrayList<LinkedTreeMap<String, String>> keyListOut =
        parseOutputIntoArrayList();
    Assert.assertEquals(testKeys.size(), keyListOut.size());
    final HashSet<String> keySet = new HashSet<>(testKeys);
    keyListOut.forEach(map -> keySet.remove(map.get("name")));
    // Should have found all the keys put for this test
    Assert.assertEquals(0, keySet.size());

    // Clean up
    testKeys.forEach(key -> execute(ozoneShell, new String[] {
        "key", "delete", keyPathPrefix + key}));
    testBuckets.forEach(bucket -> execute(ozoneShell, new String[] {
        "bucket", "delete", firstVolumePrefix + bucket}));
    testVolumes.forEach(vol -> execute(ozoneShell, new String[] {
        "volume", "delete", vol}));
  }

  @Test
  public void testClientBucketLayoutValidation() {
    String volName = "/vol-" + UUID.randomUUID();
    String[] args =
        new String[]{"volume", "create", "o3://" + omServiceId + volName};
    execute(ozoneShell, args);

    args = new String[]{
        "bucket", "create", "o3://" + omServiceId + volName + "/buck-1",
        "--layout", ""
    };
    try {
      execute(ozoneShell, args);
      Assert.fail("Should throw exception on unsupported bucket layouts!");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "expected one of [FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY] ",
          e);
    }

    args = new String[]{
        "bucket", "create", "o3://" + omServiceId + volName + "/buck-2",
        "--layout", "INVALID"
    };
    try {
      execute(ozoneShell, args);
      Assert.fail("Should throw exception on unsupported bucket layouts!");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          "expected one of [FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY] ",
          e);
    }
  }

  @Test
  public void testListAllKeys()
      throws Exception {
    String volumeName = "vollst";
    // Create volume vollst
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId +
          OZONE_URI_DELIMITER + volumeName};
    execute(ozoneShell, args);
    out.reset();

    // Create bucket bucket1
    args = new String[]{"bucket", "create", "o3://" + omServiceId +
          OZONE_URI_DELIMITER + volumeName + "/bucket1"};
    execute(ozoneShell, args);
    out.reset();

    // Insert 120 keys into bucket1
    String keyName = OZONE_URI_DELIMITER + volumeName + "/bucket1" +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 120; i++) {
      args = new String[]{
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }

    out.reset();
    // Number of keys should return less than 120(100 by default)
    args =
        new String[]{"key", "list", volumeName};
    execute(ozoneShell, args);
    Assert.assertTrue(getNumOfKeys() < 120);

    out.reset();
    // Use --all option to get all the keys
    args =
        new String[]{"key", "list", "--all", volumeName};
    execute(ozoneShell, args);
    // Number of keys returned should be 120
    Assert.assertEquals(120, getNumOfKeys());
  }

  @Test
  public void testVolumeListKeys()
      throws Exception {
    String volume1 = "volx";
    // Create volume volx
    // Create bucket bucket1 with layout FILE_SYSTEM_OPTIMIZED
    // Insert 100 keys into it
    generateKeys(OZONE_URI_DELIMITER + volume1,
        "/bucketfso",
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());

    // Create OBS bucket in volx
    String[] args = new String[]{"bucket", "create", volume1 + "/bucketobs"};
    execute(ozoneShell, args);
    out.reset();

    // Insert 5 keys into OBS bucket
    String keyName = OZONE_URI_DELIMITER + volume1 + "/bucketobs" +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 5; i++) {
      args = new String[]{
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
    out.reset();

    // Create Legacy bucket in volx
    args = new String[]{"bucket", "create", volume1 + "/bucketlegacy"};
    execute(ozoneShell, args);
    out.reset();

    // Insert 5 keys into legacy bucket
    keyName = OZONE_URI_DELIMITER + volume1 + "/bucketlegacy" +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 5; i++) {
      args = new String[]{
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
    out.reset();
    args =
        new String[]{"key", "list", "-l", "200", volume1};
    execute(ozoneShell, args);
    // Total keys should be 100+5+5=110
    Assert.assertEquals(110, getNumOfKeys());
    out.reset();

    // Try listkeys on non-existing volume
    String volume2 = "voly";
    final String[] args1 =
        new String[]{"key", "list", volume2};
    execute(ozoneShell, args);
    LambdaTestUtils.intercept(ExecutionException.class,
        "VOLUME_NOT_FOUND", () -> execute(ozoneShell, args1));
  }
}
