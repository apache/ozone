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
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.fs.ozone.OzoneFsShell;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Strings;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
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

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  private static File baseDir;
  private static File testFile;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static OzoneShell ozoneShell = null;
  private static OzoneAdmin ozoneAdminShell = null;

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
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestOzoneShellHA.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    testFile = new File(path + OzoneConsts.OZONE_URI_DELIMITER + "testFile");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    ozoneShell = new OzoneShell();
    ozoneAdminShell = new OzoneAdmin();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    conf.setQuietMode(false);
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

  @Before
  public void setup() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(out, false, UTF_8.name()));
    System.setErr(new PrintStream(err, false, UTF_8.name()));
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
    assert(numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = OmUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert(omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(OmUtils.addKeySuffixes(
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
  private void generateKeys(String volumeName, String bucketName) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    args = new String[] {
        "bucket", "create", "o3://" + omServiceId + volumeName + bucketName};
    execute(ozoneShell, args);

    String keyName = volumeName + bucketName +
        OzoneConsts.OZONE_URI_DELIMITER + "key";
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
    return out.toString(UTF_8.name()).split("key").length - 1;
  }

  /**
   * Helper function to generate buckets for testing shell command of buckets.
   */
  private void generateBuckets(String volumeName, int numOfBuckets) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    String bucketName = volumeName + OzoneConsts.OZONE_URI_DELIMITER + "bucket";
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
    return out.toString(UTF_8.name())
        .split(bucketPrefix).length - 1;
  }


  /**
   * Tests ozone sh command URI parsing with volume and bucket create commands.
   */
  @Test
  public void testOzoneShCmdURIs() {


    // Get leader OM node RPC address from ozone.om.address.omServiceId.omNode
    String omLeaderNodeId = getLeaderOMNodeId();
    String omLeaderNodeAddrKey = OmUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omLeaderNodeId);
    String omLeaderNodeAddr = conf.get(omLeaderNodeAddrKey);
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
    generateKeys("/volume4", "/bucket");
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
    Assert.assertEquals(0, out.size());
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
    Assert.assertEquals(100, getNumOfBuckets("bucket"));

    // Test case 2: test listing buckets for setting --start with last bucket.
    // ozone sh bucket list /volume5 --start=bucket99 /volume5
    // Expectation: Get empty list.
    final String startBucket = "--start=bucket99";
    out.reset();
    args = new String[] {"bucket", "list", startBucket, destinationVolume};
    execute(ozoneShell, args);
    Assert.assertEquals(0, out.size());
    Assert.assertEquals(0, getNumOfBuckets("bucket"));
  }

  /**
   * Test ozone admin list command.
   */
  @Test
  public void testOzoneAdminCmdList() throws UnsupportedEncodingException {
    // Part of listing keys test.
    generateKeys("/volume6", "/bucket");
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

  @Test
  public void testDeleteToTrashOrSkipTrash() throws Exception {
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    OzoneConfiguration clientConf = getClientConfForOFS(hostPrefix, conf);
    OzoneFsShell shell = new OzoneFsShell(clientConf);
    FileSystem fs = FileSystem.get(clientConf);
    final String strDir1 = hostPrefix + "/volumed2t/bucket1/dir1";
    // Note: CURRENT is also privately defined in TrashPolicyDefault
    final Path trashCurrent = new Path("Current");

    final String strKey1 = strDir1 + "/key1";
    final Path pathKey1 = new Path(strKey1);
    final Path trashPathKey1 = Path.mergePaths(new Path(
        new OFSPath(strKey1).getTrashRoot(), trashCurrent), pathKey1);

    final String strKey2 = strDir1 + "/key2";
    final Path pathKey2 = new Path(strKey2);
    final Path trashPathKey2 = Path.mergePaths(new Path(
        new OFSPath(strKey2).getTrashRoot(), trashCurrent), pathKey2);

    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p", strDir1});
      Assert.assertEquals(0, res);

      // Check delete to trash behavior
      res = ToolRunner.run(shell, new String[]{"-touch", strKey1});
      Assert.assertEquals(0, res);
      // Verify key1 creation
      FileStatus statusPathKey1 = fs.getFileStatus(pathKey1);
      Assert.assertEquals(strKey1, statusPathKey1.getPath().toString());
      // rm without -skipTrash. since trash interval > 0, should moved to trash
      res = ToolRunner.run(shell, new String[]{"-rm", strKey1});
      Assert.assertEquals(0, res);
      // Verify that the file is moved to the correct trash location
      FileStatus statusTrashPathKey1 = fs.getFileStatus(trashPathKey1);
      // It'd be more meaningful if we actually write some content to the file
      Assert.assertEquals(
          statusPathKey1.getLen(), statusTrashPathKey1.getLen());
      Assert.assertEquals(
          fs.getFileChecksum(pathKey1), fs.getFileChecksum(trashPathKey1));

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
    }
  }

  @Test
  @SuppressWarnings("methodlength")
  public void testShQuota() throws Exception {
    ObjectStore objectStore = cluster.getClient().getObjectStore();

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
    args = new String[]{"volume", "clrquota", "vol4", "--space-quota",
        "--namespace-quota"};
    execute(ozoneShell, args);
    assertEquals(-1, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol4").getQuotaInNamespace());
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
}
