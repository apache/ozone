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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.LEGACY;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Strings;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.ozone.OzoneFsShell;
import org.apache.hadoop.fs.ozone.OzoneTrashPolicy;
import org.apache.hadoop.hdds.JsonTestUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.service.OpenKeyCleanupService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

/**
 * This class tests Ozone sh shell command.
 * Inspired by TestS3Shell
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class TestOzoneShellHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShellHA.class);

  private static final String DEFAULT_ENCODING = UTF_8.name();
  @TempDir
  private static java.nio.file.Path path;
  @TempDir
  private static File kmsDir;
  private static File testFile;
  private static String testFilePathString;
  private static MiniOzoneHAClusterImpl cluster = null;
  private static MiniKMS miniKMS;
  private static OzoneClient client;
  private OzoneShell ozoneShell = null;
  private OzoneAdmin ozoneAdminShell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  private static String omServiceId;
  private static int numOfOMs;

  private static OzoneConfiguration ozoneConfiguration;

  @BeforeAll
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
    startKMS();
    startCluster(conf);
  }

  protected static void startKMS() throws Exception {
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
    miniKMS.start();
  }

  protected static void startCluster(OzoneConfiguration conf) throws Exception {

    testFilePathString = path + OZONE_URI_DELIMITER + "testFile";
    testFile = new File(testFilePathString);
    FileUtils.touch(testFile);

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    final int numDNs = 5;
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI(miniKMS));
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 10);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT, 1);
    ozoneConfiguration = conf;
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(numDNs);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
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

    if (miniKMS != null) {
      miniKMS.stop();
    }
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    ozoneShell = new OzoneShell();
    ozoneAdminShell = new OzoneAdmin();
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  protected void execute(GenericCli shell, String[] args) {
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
      Exception ex = assertThrows(Exception.class, () -> execute(shell, args));
      if (!Strings.isNullOrEmpty(expectedError)) {
        Throwable exceptionToCheck = ex;
        if (exceptionToCheck.getCause() != null) {
          exceptionToCheck = exceptionToCheck.getCause();
        }
        assertThat(exceptionToCheck.getMessage()).contains(expectedError);
      }
    }
  }

  /**
   * @return the leader OM's Node ID in the MiniOzoneHACluster.
   */
  private String getLeaderOMNodeId() {
    OzoneManager omLeader = cluster.getOMLeader();
    assertNotNull(omLeader, "There should be a leader OM at this point.");
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
  protected void generateKeys(String volumeName, String bucketName,
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
  protected int getNumOfKeys() throws UnsupportedEncodingException {
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
   * Parse output into ArrayList with Jackson.
   * @return ArrayList
   */
  private List<Map<String, Object>> parseOutputIntoArrayList() throws IOException {
    return JsonTestUtils.readTreeAsListOfMaps(out.toString(DEFAULT_ENCODING));
  }

  @Test
  public void testRATISTypeECReplication() {
    String[] args = new String[]{"bucket", "create", "/vol/bucket",
        "--type=" + ReplicationType.RATIS, "--replication=rs-3-2-1024k"};
    Throwable t = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, args));
    Throwable c = t.getCause();
    assertInstanceOf(IllegalArgumentException.class, c);
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
  public void testOzoneShCmdList() throws IOException {
    // Part of listing keys test.
    generateKeys("/volume4", "/bucket", "");
    final String destinationBucket = "o3://" + omServiceId + "/volume4/bucket";

    // Test case 1: test listing keys
    // ozone sh key list /volume4/bucket
    // Expectation: Get list including all keys.
    String[] args = new String[] {"key", "list", destinationBucket};
    out.reset();
    execute(ozoneShell, args);
    assertEquals(100, getNumOfKeys());

    // Test case 2: test listing keys for setting --start with last key.
    // ozone sh key list --start=key99 /volume4/bucket
    // Expectation: Get empty list.
    final String startKey = "--start=key99";
    args = new String[] {"key", "list", startKey, destinationBucket};
    out.reset();
    execute(ozoneShell, args);
    // Expect empty JSON array
    assertEquals(0, parseOutputIntoArrayList().size());
    assertEquals(0, getNumOfKeys());

    // Part of listing buckets test.
    generateBuckets("/volume5", 100);
    final String destinationVolume = "o3://" + omServiceId + "/volume5";

    // Test case 1: test listing buckets.
    // ozone sh bucket list /volume5
    // Expectation: Get list including all buckets.
    args = new String[] {"bucket", "list", destinationVolume};
    out.reset();
    execute(ozoneShell, args);
    assertEquals(100, getNumOfBuckets("testbucket"));

    // Test case 2: test listing buckets for setting --start with last bucket.
    // ozone sh bucket list /volume5 --start=bucket99 /volume5
    // Expectation: Get empty list.
    final String startBucket = "--start=testbucket99";
    out.reset();
    args = new String[] {"bucket", "list", startBucket, destinationVolume};
    execute(ozoneShell, args);
    // Expect empty JSON array
    assertEquals(0, parseOutputIntoArrayList().size());
    assertEquals(0, getNumOfBuckets("testbucket"));
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

  @Test
  public void testAdminCmdListOpenFiles()
      throws IOException, InterruptedException, TimeoutException {

    OzoneConfiguration conf = cluster.getConf();
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;

    OzoneConfiguration clientConf = getClientConfForOFS(hostPrefix, conf);
    clientConf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    clientConf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
    FileSystem fs = FileSystem.get(clientConf);

    assertNotEquals(fs.getConf().get(OZONE_FS_HSYNC_ENABLED),
        "false", OZONE_FS_HSYNC_ENABLED + " is set to false " +
            "by external force. Must be true to allow hsync to function");

    final String volumeName = "volume-lof";
    final String bucketName = "buck1";

    String dir1 = hostPrefix +
        OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName +
        OM_KEY_PREFIX + "dir1";
    // Create volume, bucket, dir
    assertTrue(fs.mkdirs(new Path(dir1)));
    String keyPrefix = OM_KEY_PREFIX + "key";

    final int numKeys = 5;
    String[] keys = new String[numKeys];

    for (int i = 0; i < numKeys; i++) {
      keys[i] = dir1 + keyPrefix + i;
    }

    int pageSize = 3;
    String pathToBucket = "/" +  volumeName + "/" + bucketName;
    FSDataOutputStream[] streams = new FSDataOutputStream[numKeys];

    try {
      // Create multiple keys and hold them open
      for (int i = 0; i < numKeys; i++) {
        streams[i] = fs.create(new Path(keys[i]));
        streams[i].write(1);
      }

      // Wait for DB flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();

      String[] args = new String[] {"om", "lof",
          "--service-id", omServiceId,
          "-l", String.valueOf(numKeys + 1),  // pagination
          "-p", pathToBucket};
      // Run listopenfiles
      execute(ozoneAdminShell, args);
      String cmdRes = getStdOut();
      // Should have retrieved all 5 open keys
      for (int i = 0; i < numKeys; i++) {
        assertTrue(cmdRes.contains(keyPrefix + i));
      }

      // Try pagination
      args = new String[] {"om", "lof",
          "--service-id", omServiceId,
          "-l", String.valueOf(pageSize),  // pagination
          "-p", pathToBucket};
      execute(ozoneAdminShell, args);
      cmdRes = getStdOut();

      // Should have retrieved the 1st page only (3 keys)
      for (int i = 0; i < pageSize; i++) {
        assertTrue(cmdRes.contains(keyPrefix + i));
      }
      for (int i = pageSize; i < numKeys; i++) {
        assertFalse(cmdRes.contains(keyPrefix + i));
      }
      // No hsync'ed file/key at this point
      assertFalse(cmdRes.contains("\tYes\t"));

      // Get last line of the output which has the continuation token
      String[] lines = cmdRes.split("\n");
      String nextCmd = lines[lines.length - 1].trim();
      String kw = "--start=";
      String contToken =
          nextCmd.substring(nextCmd.lastIndexOf(kw) + kw.length());

      args = new String[] {"om", "lof",
          "--service-id", omServiceId,
          "-l", String.valueOf(pageSize),  // pagination
          "-p", pathToBucket,
          "-s", contToken};
      execute(ozoneAdminShell, args);
      cmdRes = getStdOut();

      // Should have retrieved the 2nd page only (2 keys)
      for (int i = 0; i < pageSize - 1; i++) {
        assertFalse(cmdRes.contains(keyPrefix + i));
      }
      // Note: key2 is shown in the continuation token prompt
      for (int i = pageSize - 1; i < numKeys; i++) {
        assertTrue(cmdRes.contains(keyPrefix + i));
      }

      // hsync last key
      streams[numKeys - 1].hsync();
      // Wait for flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();

      execute(ozoneAdminShell, args);
      cmdRes = getStdOut();

      // Verify that only one key is hsync'ed
      assertTrue(cmdRes.contains("\tYes\t"), "One key should be hsync'ed");
      assertTrue(cmdRes.contains("\tNo\t"), "One key should not be hsync'ed");
    } finally {
      // Cleanup
      IOUtils.closeQuietly(streams);
    }

  }

  @Test
  public void testAdminCmdListOpenFilesWithDeletedKeys()
      throws Exception {

    OzoneConfiguration conf = cluster.getConf();
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;

    OzoneConfiguration clientConf = getClientConfForOFS(hostPrefix, conf);
    clientConf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    clientConf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
    FileSystem fs = FileSystem.get(clientConf);

    assertNotEquals(fs.getConf().get(OZONE_FS_HSYNC_ENABLED),
        "false", OZONE_FS_HSYNC_ENABLED + " is set to false " +
            "by external force. Must be true to allow hsync to function");

    final String volumeName = "volume-list-del";
    final String bucketName = "buck1";

    String dir1 = hostPrefix +
        OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName +
        OM_KEY_PREFIX + "dir1";
    // Create volume, bucket, dir
    assertTrue(fs.mkdirs(new Path(dir1)));
    String keyPrefix = OM_KEY_PREFIX + "key";

    final int numKeys = 5;
    String[] keys = new String[numKeys];

    for (int i = 0; i < numKeys; i++) {
      keys[i] = dir1 + keyPrefix + i;
    }

    String pathToBucket = "/" +  volumeName + "/" + bucketName;
    FSDataOutputStream[] streams = new FSDataOutputStream[numKeys];

    try {
      // Create multiple keys and hold them open
      for (int i = 0; i < numKeys; i++) {
        streams[i] = fs.create(new Path(keys[i]));
        streams[i].write(1);
      }

      // Wait for DB flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();

      // hsync last key
      streams[numKeys - 1].hsync();
      // Wait for flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();
      final String[] args = new String[] {"om", "lof", "--service-id",
          omServiceId, "--show-deleted", "-p", pathToBucket};

      execute(ozoneAdminShell, args);
      String cmdRes = getStdOut();

      // Verify that key is hsync'ed
      assertTrue(cmdRes.contains("\tYes\t\tNo"), "key should be hsync'ed and not deleted");

      // Verify json output
      String[] args1 = new String[] {"om", "lof", "--service-id", omServiceId, "--show-deleted",
          "--json", "-p", pathToBucket};
      execute(ozoneAdminShell, args1);
      cmdRes = getStdOut();

      assertTrue(!cmdRes.contains(OzoneConsts.DELETED_HSYNC_KEY),
          "key should not have deletedHsyncKey metadata");

      // Suspend open key cleanup service so that key remains in openKeyTable for verification
      OpenKeyCleanupService openKeyCleanupService =
          (OpenKeyCleanupService) cluster.getOzoneManager().getKeyManager().getOpenKeyCleanupService();
      openKeyCleanupService.suspend();
      OzoneFsShell shell = new OzoneFsShell(clientConf);
      // Delete directory dir1
      ToolRunner.run(shell, new String[]{"-rm", "-R", "-skipTrash", dir1});

      GenericTestUtils.waitFor(() -> {
        try {
          execute(ozoneAdminShell, args);
          String cmdRes1 = getStdOut();
          // When directory purge request is triggered it should add DELETED_HSYNC_KEY metadata in hsync openKey
          // And list open key should show as deleted
          return cmdRes1.contains("\tYes\t\tYes");
        } catch (Throwable t) {
          LOG.warn("Failed to list open key", t);
          return false;
        }
      }, 1000, 10000);

      // Now check json output
      execute(ozoneAdminShell, args1);
      cmdRes = getStdOut();
      assertTrue(cmdRes.contains(OzoneConsts.DELETED_HSYNC_KEY),
          "key should have deletedHsyncKey metadata");

      // Verify result should not have deleted hsync keys when --show-deleted is not in the command argument
      String[] args2 = new String[] {"om", "lof", "--service-id", omServiceId, "-p", pathToBucket};
      execute(ozoneAdminShell, args2);
      cmdRes = getStdOut();
      // Verify that deletedHsyncKey is not in the result
      assertTrue(!cmdRes.contains("\tYes\t\tYes"), "key should be hsync'ed and not deleted");

      // Verify with json result
      args2 = new String[] {"om", "lof", "--service-id", omServiceId, "--json", "-p", pathToBucket};
      execute(ozoneAdminShell, args2);
      cmdRes = getStdOut();
      // Verify that deletedHsyncKey is not in the result
      assertTrue(!cmdRes.contains(OzoneConsts.DELETED_HSYNC_KEY),
          "key should not have deletedHsyncKey metadata");

    }  finally {
      // Cleanup
      IOUtils.closeQuietly(streams);
    }
  }

  @Test
  public void testAdminCmdListOpenFilesWithOverwrittenKeys()
      throws Exception {

    OzoneConfiguration conf = cluster.getConf();
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;

    OzoneConfiguration clientConf = getClientConfForOFS(hostPrefix, conf);
    clientConf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    clientConf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
    FileSystem fs = FileSystem.get(clientConf);

    assertNotEquals(fs.getConf().get(OZONE_FS_HSYNC_ENABLED),
        "false", OZONE_FS_HSYNC_ENABLED + " is set to false " +
            "by external force. Must be true to allow hsync to function");

    final String volumeName = "volume-list-del";
    final String bucketName = "buck1";

    String dir1 = hostPrefix +
        OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName +
        OM_KEY_PREFIX + "dir1";
    // Create volume, bucket, dir
    assertTrue(fs.mkdirs(new Path(dir1)));
    String keyPrefix = OM_KEY_PREFIX + "key";

    final int numKeys = 5;
    String[] keys = new String[numKeys];

    for (int i = 0; i < numKeys; i++) {
      keys[i] = dir1 + keyPrefix + i;
    }

    String pathToBucket = "/" +  volumeName + "/" + bucketName;
    FSDataOutputStream[] streams = new FSDataOutputStream[numKeys];

    try {
      // Create multiple keys and hold them open
      for (int i = 0; i < numKeys; i++) {
        streams[i] = fs.create(new Path(keys[i]));
        streams[i].write(1);
      }

      // Wait for DB flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();

      // hsync last key
      streams[numKeys - 1].hsync();
      // Wait for flush
      cluster.getOzoneManager().awaitDoubleBufferFlush();
      final String[] args = new String[] {"om", "lof", "--service-id",
          omServiceId, "--show-deleted", "--show-overwritten", "-p", pathToBucket};

      execute(ozoneAdminShell, args);
      String cmdRes = getStdOut();

      // Verify that key is hsync'ed
      assertTrue(cmdRes.contains("\tYes\t\tNo\t\tNo"), "key should be hsync'ed and not deleted, not overwritten");

      execute(ozoneAdminShell, new String[] {"om", "lof", "--service-id",
          omServiceId, "--show-overwritten", "-p", pathToBucket});
      cmdRes = getStdOut();
      // Verify that key is hsync'ed
      assertTrue(cmdRes.contains("\tYes\t\tNo"), "key should be hsync'ed and not overwritten");

      // Verify json output
      String[] args1 = new String[] {"om", "lof", "--service-id", omServiceId, "--show-deleted", "--show-overwritten",
          "--json", "-p", pathToBucket};
      execute(ozoneAdminShell, args1);
      cmdRes = getStdOut();

      assertTrue(!cmdRes.contains(OzoneConsts.DELETED_HSYNC_KEY),
          "key should not have deletedHsyncKey metadata");
      assertTrue(!cmdRes.contains(OzoneConsts.OVERWRITTEN_HSYNC_KEY),
          "key should not have overwrittenHsyncKey metadata");

      // Suspend open key cleanup service so that key remains in openKeyTable for verification
      OpenKeyCleanupService openKeyCleanupService =
          (OpenKeyCleanupService) cluster.getOzoneManager().getKeyManager().getOpenKeyCleanupService();
      openKeyCleanupService.suspend();
      // overwrite last key
      try (FSDataOutputStream os = fs.create(new Path(keys[numKeys - 1]))) {
        os.write(2);
      }

      GenericTestUtils.waitFor(() -> {
        try {
          execute(ozoneAdminShell, args);
          String cmdRes1 = getStdOut();
          // When hsync file is overwritten, it should add OVERWRITTEN_HSYNC_KEY metadata in hsync openKey
          // And list open key should show as overwritten
          return cmdRes1.contains("\tYes\t\tNo\t\tYes");
        } catch (Throwable t) {
          LOG.warn("Failed to list open key", t);
          return false;
        }
      }, 1000, 10000);

      // Now check json output
      execute(ozoneAdminShell, args1);
      cmdRes = getStdOut();
      assertTrue(!cmdRes.contains(OzoneConsts.DELETED_HSYNC_KEY),
          "key should not have deletedHsyncKey metadata");
      assertTrue(cmdRes.contains(OzoneConsts.OVERWRITTEN_HSYNC_KEY),
          "key should have overwrittenHsyncKey metadata");

      // Verify result should not have overwritten hsync keys when --show-overwritten is not in the command argument
      String[] args2 = new String[] {"om", "lof", "--service-id", omServiceId, "-p", pathToBucket};
      execute(ozoneAdminShell, args2);
      cmdRes = getStdOut();
      // Verify that overwrittenHsyncKey is not in the result
      assertTrue(!cmdRes.contains("\tYes\t\tYes"), "key should be hsync'ed and not overwritten");

      // Verify with json result
      args2 = new String[] {"om", "lof", "--service-id", omServiceId, "--json", "-p", pathToBucket};
      execute(ozoneAdminShell, args2);
      cmdRes = getStdOut();
      // Verify that overwrittenHsyncKey is not in the result
      assertTrue(!cmdRes.contains(OzoneConsts.OVERWRITTEN_HSYNC_KEY),
          "key should not have overwrittenHsyncKey metadata");

    }  finally {
      // Cleanup
      IOUtils.closeQuietly(streams);
    }
  }

  /**
   * Return stdout as a String, then clears existing output.
   */
  private String getStdOut() throws UnsupportedEncodingException {
    String res = out.toString(UTF_8.name());
    out.reset();
    return res;
  }

  @Test
  public void testOzoneAdminCmdListAllContainer()
      throws UnsupportedEncodingException {
    String[] args = new String[] {"container", "create", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort()};
    for (int i = 0; i < 2; i++) {
      execute(ozoneAdminShell, args);
    }

    String[] args1 = new String[] {"container", "list", "-c", "1", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort()};
    execute(ozoneAdminShell, args1);
    //results will be capped at the maximum allowed count
    assertEquals(1, getNumOfContainers());
    out.reset();
    err.reset();
    String[] args2 = new String[] {"container", "list", "-a", "--scm",
        "localhost:" + cluster.getStorageContainerManager().getClientRpcPort()};
    execute(ozoneAdminShell, args2);
    //Lists all containers, at least the two created for this method
    assertThat(getNumOfContainers())
        .isGreaterThanOrEqualTo(2);
  }

  private int getNumOfContainers()
      throws UnsupportedEncodingException {
    return out.toString(DEFAULT_ENCODING).split("\"containerID\" :").length - 1;
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
   * trash testing with OzoneTrashPolicy.
   * @param hostPrefix Scheme + Authority. e.g. ofs://om-service-test1
   * @param configuration Server config to generate client config from.
   * @return Config ofs configuration added with fs.trash.classname
   * = OzoneTrashPolicy.
   */
  private OzoneConfiguration getClientConfForOzoneTrashPolicy(
          String hostPrefix, OzoneConfiguration configuration) {
    OzoneConfiguration clientConf =
            getClientConfForOFS(hostPrefix, configuration);
    clientConf.setClass("fs.trash.classname", OzoneTrashPolicy.class,
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
      assertEquals(0, res);

      // Check delete to trash behavior
      res = ToolRunner.run(shell, new String[]{"-touch", strKey1});
      assertEquals(0, res);
      // Verify key1 creation
      FileStatus statusPathKey1 = fs.getFileStatus(pathKey1);

      FileChecksum previousFileChecksum = fs.getFileChecksum(pathKey1);

      assertEquals(strKey1, statusPathKey1.getPath().toString());
      // rm without -skipTrash. since trash interval > 0, should moved to trash
      res = ToolRunner.run(shell, new String[]{"-rm", strKey1});
      assertEquals(0, res);

      FileChecksum afterFileChecksum = fs.getFileChecksum(trashPathKey1);

      // Verify that the file is moved to the correct trash location
      FileStatus statusTrashPathKey1 = fs.getFileStatus(trashPathKey1);
      // It'd be more meaningful if we actually write some content to the file
      assertEquals(
          statusPathKey1.getLen(), statusTrashPathKey1.getLen());
      assertEquals(previousFileChecksum, afterFileChecksum);

      // Check delete skip trash behavior
      res = ToolRunner.run(shell, new String[]{"-touch", strKey2});
      assertEquals(0, res);
      // Verify key2 creation
      FileStatus statusPathKey2 = fs.getFileStatus(pathKey2);
      assertEquals(strKey2, statusPathKey2.getPath().toString());
      // rm with -skipTrash
      res = ToolRunner.run(shell, new String[]{"-rm", "-skipTrash", strKey2});
      assertEquals(0, res);
      // Verify that the file is NOT moved to the trash location
      assertThrows(FileNotFoundException.class,
          () -> fs.getFileStatus(trashPathKey2));
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

    try {
      int res;
      // Test orphan link bucket when source volume removed
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/vol1/bucket1"});
      assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/linkvol"});
      assertEquals(0, res);

      String[] args =
          new String[]{"bucket", "link", "/vol1/bucket1",
              "/linkvol/linkbuck"};
      execute(ozoneShell, args);

      args =
          new String[] {"volume", "delete", "vol1", "-r", "--yes"};
      execute(ozoneShell, args);
      out.reset();
      OMException omExecution = assertThrows(OMException.class,
          () -> client.getObjectStore().getVolume("vol1"));
      assertEquals(VOLUME_NOT_FOUND, omExecution.getResult());

      res = ToolRunner.run(shell, new String[]{"-ls",
          hostPrefix + "/linkvol"});
      assertEquals(0, res);

      args = new String[] {"bucket", "delete", "linkvol"
              + OZONE_URI_DELIMITER + "linkbuck"};
      execute(ozoneShell, args);
      out.reset();

      args = new String[] {"volume", "delete", "linkvol"};
      execute(ozoneShell, args);
      out.reset();
      omExecution = assertThrows(OMException.class,
          () -> client.getObjectStore().getVolume("linkvol"));
      assertEquals(VOLUME_NOT_FOUND, omExecution.getResult());

      // Test orphan link bucket when only source bucket removed
      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/vol1/bucket1"});
      assertEquals(0, res);

      res = ToolRunner.run(shell, new String[]{"-mkdir", "-p",
          hostPrefix + "/linkvol"});
      assertEquals(0, res);

      args = new String[]{"bucket", "link", "/vol1/bucket1",
          "/linkvol/linkbuck"};
      execute(ozoneShell, args);

      res = ToolRunner.run(shell, new String[]{"-rm", "-R", "-f",
          "-skipTrash", hostPrefix + "/vol1/bucket1"});
      assertEquals(0, res);

      args = new String[] {"bucket", "delete", "linkvol"
              + OZONE_URI_DELIMITER + "linkbuck"};
      execute(ozoneShell, args);
      out.reset();

      args = new String[] {"volume", "delete", "linkvol"};
      execute(ozoneShell, args);
      out.reset();
      omExecution = assertThrows(OMException.class,
          () -> client.getObjectStore().getVolume("linkvol"));
      assertEquals(VOLUME_NOT_FOUND, omExecution.getResult());
    } finally {
      shell.close();
    }
  }

  @Test
  public void testListBucket() throws Exception {
    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    OzoneConfiguration clientConf =
            getClientConfForOFS(hostPrefix, cluster.getConf());
    int pageSize = 20;
    clientConf.setInt(OZONE_FS_LISTING_PAGE_SIZE, pageSize);
    OzoneFsShell shell = new OzoneFsShell(clientConf);

    String volName = "testlistbucket";
    int numBuckets = pageSize;

    try {
      generateBuckets("/" + volName, numBuckets);
      out.reset();
      int res = ToolRunner.run(shell, new String[]{"-ls", "/" + volName});
      assertEquals(0, res);
      String r = out.toString(DEFAULT_ENCODING);
      assertThat(r).matches("(?s)^Found " + numBuckets + " items.*");

    } finally {
      shell.close();
    }
  }

  @Test
  public void testDeleteTrashNoSkipTrash() throws Exception {

    // Test delete from Trash directory removes item from filesystem

    // setup configuration to use OzoneTrashPolicy
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
    assertEquals(0, res);

    // create key: key1 belonging to bucket1
    res = ToolRunner.run(shell, keyArgs);
    assertEquals(0, res);

    // check directory listing for bucket1 contains 1 key
    out.reset();
    execute(ozoneShell, listArgs);
    assertEquals(1, getNumOfKeys());

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
      assertEquals(0, res);

      LOG.info("Executing testDeleteTrashNoSkipTrash: key1 deleted moved to"
              + " Trash: " + trashPathKey1.toString());
      fs.getFileStatus(trashPathKey1);

      LOG.info("Executing testDeleteTrashNoSkipTrash: deleting trash FsShell "
              + "with args{}: ", Arrays.asList(rmTrashArgs));
      res = ToolRunner.run(shell, rmTrashArgs);
      assertEquals(0, res);

      out.reset();
      // once trash is is removed, trash should be deleted from filesystem
      execute(ozoneShell, listArgs);
      assertEquals(0, getNumOfKeys());

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


    // Test negative scenarios for --space-quota and --namespace-quota option
    args = new String[]{"volume", "create", "vol5", "--space-quota"};
    executeWithError(ozoneShell, args,
        "Missing required parameter for option " +
        "'--space-quota' (<quotaInBytes>)");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--space-quota", "-1"};
    executeWithError(ozoneShell, args, "Invalid value for space quota: -1");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--space-quota", "test"};
    executeWithError(ozoneShell, args, "test is invalid. " +
        "The quota value should be a positive integer " +
        "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--space-quota", "1.5GB"};
    executeWithError(ozoneShell, args, "1.5GB is invalid. " +
        "The quota value should be a positive integer " +
        "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--namespace-quota"};
    executeWithError(ozoneShell, args,
        "Missing required parameter for option " +
        "'--namespace-quota' (<quotaInNamespace>)");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--namespace-quota", "-1"};
    executeWithError(ozoneShell, args,
        "Invalid value for namespace quota: -1");
    out.reset();

    args = new String[]{"volume", "create", "vol5"};
    execute(ozoneShell, args);
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5", "--space-quota"};
    executeWithError(ozoneShell, args,
        "Missing required parameter for option " +
        "'--space-quota' (<quotaInBytes>)");
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5",
        "--space-quota", "-1"};
    executeWithError(ozoneShell, args,
        "Invalid value for space quota: -1");
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5",
        "--space-quota", "test"};
    executeWithError(ozoneShell, args, "test is invalid. " +
        "The quota value should be a positive integer " +
        "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5",
        "--space-quota", "1.5GB"};
    executeWithError(ozoneShell, args, "1.5GB is invalid. " +
        "The quota value should be a positive integer " +
        "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5", "--namespace-quota"};
    executeWithError(ozoneShell, args,
        "Missing required parameter for option " +
        "'--namespace-quota' (<quotaInNamespace>)");
    out.reset();

    args = new String[]{"volume", "create", "vol5", "--namespace-quota", "-1"};
    executeWithError(ozoneShell, args,
        "Invalid value for namespace quota: -1");
    out.reset();

    args = new String[]{"bucket", "create", "vol5/buck5"};
    execute(ozoneShell, args);
    out.reset();

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

    // Test set volume quota to invalid values.
    String[] volumeArgs1 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "0GB"};
    ExecutionException eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, volumeArgs1));
    assertThat(eException.getMessage())
        .contains("Invalid value for space quota");
    out.reset();

    String[] volumeArgs2 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "-1GB"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, volumeArgs2));
    assertThat(eException.getMessage())
        .contains("Invalid value for space quota");
    out.reset();

    String[] volumeArgs3 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "test"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, volumeArgs3));
    assertThat(eException.getMessage())
        .contains("test is invalid. " +
            "The quota value should be a positive integer " +
            "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    String[] volumeArgs4 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "1.5GB"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, volumeArgs4));
    assertThat(eException.getMessage())
        .contains("1.5GB is invalid. " +
            "The quota value should be a positive integer " +
            "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    String[] volumeArgs5 = new String[]{"volume", "setquota", "vol4",
        "--space-quota"};
    MissingParameterException mException = assertThrows(
        MissingParameterException.class,
        () -> execute(ozoneShell, volumeArgs5));
    assertThat(mException.getMessage())
        .contains("Missing required parameter");
    out.reset();

    String[] volumeArgs6 = new String[]{"volume", "setquota", "vol4",
        "--namespace-quota", "0"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, volumeArgs6));
    assertThat(eException.getMessage())
        .contains("Invalid value for namespace quota");
    out.reset();

    String[] volumeArgs7 = new String[]{"volume", "setquota", "vol4",
        "--namespace-quota"};
    mException = assertThrows(MissingParameterException.class,
        () -> execute(ozoneShell, volumeArgs7));
    assertThat(mException.getMessage())
        .contains("Missing required parameter");
    out.reset();

    // Test set bucket quota to invalid values
    String[] bucketArgs1 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "0GB"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, bucketArgs1));
    assertThat(eException.getMessage())
        .contains("Invalid value for space quota");
    out.reset();

    String[] bucketArgs2 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "-1GB"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, bucketArgs2));
    assertThat(eException.getMessage())
        .contains("Invalid value for space quota");
    out.reset();

    String[] bucketArgs3 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "test"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, bucketArgs3));
    assertThat(eException.getMessage())
        .contains("test is invalid. " +
            "The quota value should be a positive integer " +
            "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    String[] bucketArgs4 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "1.5GB"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, bucketArgs4));
    assertThat(eException.getMessage())
        .contains("1.5GB is invalid. " +
            "The quota value should be a positive integer " +
            "with byte numeration(B, KB, MB, GB and TB)");
    out.reset();

    String[] bucketArgs5 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota"};
    mException = assertThrows(MissingParameterException.class,
        () -> execute(ozoneShell, bucketArgs5));
    assertThat(mException.getMessage())
        .contains("Missing required parameter");
    out.reset();

    String[] bucketArgs6 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--namespace-quota", "0"};
    eException = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, bucketArgs6));
    assertThat(eException.getMessage())
        .contains("Invalid value for namespace quota");
    out.reset();

    String[] bucketArgs7 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--namespace-quota"};
    mException = assertThrows(MissingParameterException.class,
        () -> execute(ozoneShell, bucketArgs7));
    assertThat(mException.getMessage())
        .contains("Missing required parameter");
    out.reset();

    // Test incompatible volume-bucket quota
    args = new String[]{"volume", "create", "vol6"};
    execute(ozoneShell, args);
    out.reset();

    args = new String[]{"bucket", "create", "vol6/buck6"};
    execute(ozoneShell, args);
    out.reset();

    args = new String[]{"volume", "setquota", "vol6", "--space-quota", "1000B"};
    executeWithError(ozoneShell, args, "Can not set volume space quota " +
        "on volume as some of buckets in this volume have no quota set");
    out.reset();

    args = new String[]{"bucket", "setquota", "vol6/buck6", "--space-quota", "1000B"};
    execute(ozoneShell, args);
    out.reset();

    args = new String[]{"volume", "setquota", "vol6", "--space-quota", "2000B"};
    execute(ozoneShell, args);
    out.reset();

    args = new String[]{"bucket", "create", "vol6/buck62"};
    executeWithError(ozoneShell, args, "Bucket space quota in this " +
        "volume should be set as volume space quota is already set.");
    out.reset();

    args = new String[]{"bucket", "create", "vol6/buck62", "--space-quota", "2000B"};
    executeWithError(ozoneShell, args, "Total buckets quota in this volume " +
        "should not be greater than volume quota : the total space quota is set to:3000. " +
        "But the volume space quota is:2000");
    out.reset();

    // Test set bucket spaceQuota or nameSpaceQuota to normal value.
    String[] bucketArgs8 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "1000B"};
    execute(ozoneShell, bucketArgs8);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(-1, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    String[] bucketArgs9 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--namespace-quota", "100"};
    execute(ozoneShell, bucketArgs9);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(100, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    // test whether supports default quota unit as bytes.
    String[] bucketArgs10 = new String[]{"bucket", "setquota", "vol4/buck4",
        "--space-quota", "500"};
    execute(ozoneShell, bucketArgs10);
    out.reset();
    assertEquals(500, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInBytes());
    assertEquals(100, objectStore.getVolume("vol4")
        .getBucket("buck4").getQuotaInNamespace());

    // Test set volume quota without quota flag
    String[] bucketArgs11 = new String[]{"bucket", "setquota", "vol4/buck4"};
    executeWithError(ozoneShell, bucketArgs11,
        "At least one of the quota set flag is required");
    out.reset();

    // Test set volume spaceQuota or nameSpaceQuota to normal value.
    String[] volumeArgs8 = new String[]{"volume", "setquota", "vol4",
        "--space-quota", "1000B"};
    execute(ozoneShell, volumeArgs8);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(-1,
        objectStore.getVolume("vol4").getQuotaInNamespace());

    String[] volumeArgs9 = new String[]{"volume", "setquota", "vol4",
        "--namespace-quota", "100"};
    execute(ozoneShell, volumeArgs9);
    out.reset();
    assertEquals(1000, objectStore.getVolume("vol4").getQuotaInBytes());
    assertEquals(100,
        objectStore.getVolume("vol4").getQuotaInNamespace());

    // Test set volume quota without quota flag
    String[] volumeArgs10 = new String[]{"volume", "setquota", "vol4"};
    executeWithError(ozoneShell, volumeArgs10,
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
    objectStore.getVolume("vol5").deleteBucket("buck5");
    objectStore.deleteVolume("vol5");
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
      assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());
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
  public void testPutKeyWithECReplicationConfig() throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();
    getVolume(volumeName);
    String bucketPath =
            Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String[] args =
            new String[] {"bucket", "create", bucketPath};
    execute(ozoneShell, args);

    args = new String[] {"key", "put", "-r", "rs-3-2-1024k", "-t", "EC",
        bucketPath + Path.SEPARATOR + keyName, testFilePathString};
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
      assertInstanceOf(KeyOutputStream.class, out.getOutputStream());
      assertFalse(out.getOutputStream() instanceof ECKeyOutputStream);
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
      assertFalse(out.getOutputStream() instanceof ECKeyOutputStream);
    }

    args = new String[] {"bucket", "set-replication-config", bucketPath, "-t",
        "EC", "-r", "rs-3-2-1024k"};
    execute(ozoneShell, args);
    bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("newECKey", 1024)) {
      assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());
    }

    args = new String[] {"bucket", "set-replication-config", bucketPath, "-t",
        "RATIS", "-r", "THREE"};
    execute(ozoneShell, args);
    bucket = volume.getBucket("bucket0");
    try (OzoneOutputStream out = bucket.createKey("newNonECKey", 1024)) {
      assertFalse(out.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testSetEncryptionKey() throws Exception {
    final String volumeName = "volume111";
    getVolume(volumeName);
    String bucketPath = "/volume111/bucket0";
    String[] args = new String[]{"bucket", "create", bucketPath};
    execute(ozoneShell, args);

    OzoneVolume volume =
        client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket("bucket0");
    assertNull(bucket.getEncryptionKeyName());
    String newEncKey = "enckey1";

    KeyProvider provider = cluster.getOzoneManager().getKmsProvider();
    KeyProvider.Options options = KeyProvider.options(cluster.getConf());
    options.setDescription(newEncKey);
    options.setBitLength(128);
    provider.createKey(newEncKey, options);
    provider.flush();

    args = new String[]{"bucket", "set-encryption-key", bucketPath, "-k",
        newEncKey};
    execute(ozoneShell, args);
    assertEquals(newEncKey, volume.getBucket("bucket0").getEncryptionKeyName());
  }

  @Test
  public void testCreateBucketWithECReplicationConfigWithoutReplicationParam() {
    getVolume("volume102");
    String[] args =
        new String[] {"bucket", "create", "/volume102/bucket2", "-t", "EC"};
    try {
      execute(ozoneShell, args);
//      fail("Must throw Exception when missing replication param");
    } catch (Exception e) {
      assertEquals(
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
    assertEquals(1, getNumOfKeys());

    args = new String[] {trashConfKey, "key", "delete",
        "/volumefso1/bucket1/key5"};
    execute(ozoneShell, args);

    args = new String[] {"key", "list", "o3://" + omServiceId +
          "/volumefso1/bucket1/", "-l ", "110"};
    out.reset();
    execute(ozoneShell, args);

    // Total number of keys still 100.
    assertEquals(100, getNumOfKeys());

    // .Trash should contain 2 keys
    prefixKey = "--prefix=.Trash";
    args = new String[] {"key", "list", prefixKey, "o3://" +
          omServiceId + "/volumefso1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);
    assertEquals(2, getNumOfKeys());

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

    // Total number of keys still remain 100 as
    // delete from trash not allowed using sh command
    assertEquals(100, getNumOfKeys());

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
    assertEquals(0, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" +
          omServiceId + "/volumefso2/bucket2/"};
    out.reset();
    execute(ozoneShell, args);

    // Number of keys remain as 99
    assertEquals(99, getNumOfKeys());
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
    assertEquals(0, getNumOfKeys());

    args = new String[] {"key", "list", "o3://" +
          omServiceId + "/volumeobs1/bucket1/"};
    out.reset();
    execute(ozoneShell, args);

    assertEquals(99, getNumOfKeys());
  }

  @Test
  // Run this UT last. This interferes with testAdminCmdListOpenFiles
  @Order(Integer.MAX_VALUE)
  public void testRecursiveBucketDelete()
      throws Exception {
    String volume1 = "volume50";
    String bucket1 = "bucketfso";
    String bucket2 = "bucketobs";
    String bucket3 = "bucketlegacy";

    // Create volume volume1
    // Create bucket bucket1 with layout FILE_SYSTEM_OPTIMIZED
    // Insert some keys into it
    generateKeys(OZONE_URI_DELIMITER + volume1,
        OZONE_URI_DELIMITER + bucket1,
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());

    // Create OBS bucket in volume1
    String[] args = new String[] {"bucket", "create", "--layout",
        BucketLayout.OBJECT_STORE.toString(), volume1 +
          OZONE_URI_DELIMITER + bucket2};
    execute(ozoneShell, args);
    out.reset();

    // Insert few keys into OBS bucket
    String keyName = OZONE_URI_DELIMITER + volume1 +
        OZONE_URI_DELIMITER + bucket2 +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 5; i++) {
      args = new String[] {
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
    out.reset();

    // Create Legacy bucket in volume1
    args = new String[] {"bucket", "create", "--layout",
        BucketLayout.LEGACY.toString(), volume1 +
          OZONE_URI_DELIMITER + bucket3};
    execute(ozoneShell, args);
    out.reset();

    // Insert few keys into legacy bucket
    keyName = OZONE_URI_DELIMITER + volume1 + OZONE_URI_DELIMITER + bucket3 +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 5; i++) {
      args = new String[] {
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
    out.reset();

    // Try bucket delete without recursive
    // It should fail as bucket is not empty
    final String[] args1 = new String[] {"bucket", "delete",
        volume1 + OZONE_URI_DELIMITER + bucket1};
    ExecutionException exception = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, args1));
    OMException omException = (OMException) exception.getCause();
    assertEquals(BUCKET_NOT_EMPTY, omException.getResult());
    out.reset();

    // bucket1 should still exist
    assertEquals(client.getObjectStore()
        .getVolume(volume1).getBucket(bucket1)
        .getName(), bucket1);

    // Delete bucket1 recursively
    args = new String[]{"bucket", "delete", volume1 + OZONE_URI_DELIMITER +
          bucket1, "-r", "--yes"};
    execute(ozoneShell, args);
    out.reset();

    // Bucket1 should not exist
    omException = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume1).getBucket(bucket1));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());
    // Bucket2 and Bucket3 should still exist
    assertEquals(client.getObjectStore().getVolume(volume1)
        .getBucket(bucket2).getName(), bucket2);
    assertEquals(client.getObjectStore().getVolume(volume1)
        .getBucket(bucket3).getName(), bucket3);

    // Delete bucket2(obs) recursively.
    args = new String[]{"bucket", "delete", volume1 + OZONE_URI_DELIMITER +
          bucket2, "-r", "--yes"};
    execute(ozoneShell, args);
    out.reset();

    omException = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume1).getBucket(bucket2));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());

    // Delete bucket3(legacy) recursively.
    args = new String[] {"bucket", "delete", volume1 + OZONE_URI_DELIMITER +
          bucket3, "-r", "--yes"};
    execute(ozoneShell, args);
    out.reset();

    omException = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume1).getBucket(bucket3));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());

    // Now delete volume without recursive
    // All buckets are already deleted
    args = new String[] {"volume", "delete", volume1};
    execute(ozoneShell, args);
    out.reset();
    omException = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume1));
    assertEquals(VOLUME_NOT_FOUND, omException.getResult());
  }

  private void getVolume(String volumeName) {
    String[] args = new String[] {"volume", "create",
        "o3://" + omServiceId + "/" + volumeName};
    execute(ozoneShell, args);
  }

  public void testListVolumeBucketKeyShouldPrintValidJsonArray()
      throws IOException {

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
    final List<Map<String, Object>> volumeListOut =
        parseOutputIntoArrayList();
    // Can include s3v and volumes from other test cases that aren't cleaned up,
    //  hence >= instead of equals.
    assertThat(volumeListOut.size()).isGreaterThanOrEqualTo(testVolumes.size());
    final HashSet<String> volumeSet = new HashSet<>(testVolumes);
    volumeListOut.forEach(map -> volumeSet.remove(map.get("name")));
    // Should have found all the volumes created for this test
    assertEquals(0, volumeSet.size());

    // ozone sh bucket list jsontest-vol1
    out.reset();
    execute(ozoneShell, new String[] {"bucket", "list", firstVolumePrefix});

    // Expect valid JSON array as well
    final List<Map<String, Object>> bucketListOut =
        parseOutputIntoArrayList();
    assertEquals(testBuckets.size(), bucketListOut.size());
    final HashSet<String> bucketSet = new HashSet<>(testBuckets);
    bucketListOut.forEach(map -> bucketSet.remove(map.get("name")));
    // Should have found all the buckets created for this test
    assertEquals(0, bucketSet.size());

    // ozone sh key list jsontest-vol1/v1-bucket1
    out.reset();
    execute(ozoneShell, new String[] {"key", "list", keyPathPrefix});

    // Expect valid JSON array as well
    final List<Map<String, Object>> keyListOut =
        parseOutputIntoArrayList();
    assertEquals(testKeys.size(), keyListOut.size());
    final HashSet<String> keySet = new HashSet<>(testKeys);
    keyListOut.forEach(map -> keySet.remove(map.get("name")));
    // Should have found all the keys put for this test
    assertEquals(0, keySet.size());

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
    String[] arg1 = new String[]{"volume", "create", "o3://" + omServiceId +
          volName};
    execute(ozoneShell, arg1);

    String[] arg2 = new String[]{
        "bucket", "create", "o3://" + omServiceId + volName + "/buck-1",
        "--layout", ""
    };
    ParameterException exception = assertThrows(ParameterException.class,
        () -> execute(ozoneShell, arg2));
    assertThat(exception.getMessage())
        .contains("cannot convert '' to BucketLayout");


    String[] arg3 = new String[]{
        "bucket", "create", "o3://" + omServiceId + volName + "/buck-2",
        "--layout", "INVALID"
    };

    exception = assertThrows(ParameterException.class,
        () -> execute(ozoneShell, arg3));
    assertThat(exception.getMessage())
        .contains("cannot convert 'INVALID' to BucketLayout");
  }

  @Test
  public void testListAllKeys()
      throws Exception {
    testListAllKeysInternal("vollst");
  }

  protected void testListAllKeysInternal(String volumeName) throws Exception {
    // Create volume
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
      args = new String[]{"key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }

    out.reset();
    // Number of keys should return less than 120(100 by default)
    args = new String[]{"key", "list", volumeName};
    execute(ozoneShell, args);
    assertThat(getNumOfKeys()).isLessThan(120);

    out.reset();
    // Use --all option to get all the keys
    args = new String[]{"key", "list", "--all", volumeName};
    execute(ozoneShell, args);
    // Number of keys returned should be 120
    assertEquals(120, getNumOfKeys());
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
    String[] args = new String[]{"bucket", "create", "--layout",
        BucketLayout.OBJECT_STORE.toString(), volume1 + "/bucketobs"};
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
    args = new String[]{"bucket", "create", "--layout",
        BucketLayout.LEGACY.toString(), volume1 + "/bucketlegacy"};
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
    assertEquals(110, getNumOfKeys());
    out.reset();

    // Try listkeys on non-existing volume
    String volume2 = "voly";
    final String[] args1 =
        new String[]{"key", "list", volume2};
    execute(ozoneShell, args);
    ExecutionException execution = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, args1));
    OMException exception = (OMException) execution.getCause();
    assertEquals(VOLUME_NOT_FOUND, exception.getResult());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5})
  public void testRecursiveVolumeDelete(int threadCount)
      throws Exception {
    String volume1 = "volume10";
    String volume2 = "volume20";

    // Create volume volume1
    // Create bucket bucket1 with layout FILE_SYSTEM_OPTIMIZED
    // Insert some keys into it
    generateKeys(OZONE_URI_DELIMITER + volume1,
        "/fsobucket1",
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());

    // Create another volume volume2 with bucket and some keys into it.
    generateKeys(OZONE_URI_DELIMITER + volume2,
        "/bucket2",
        BucketLayout.FILE_SYSTEM_OPTIMIZED.toString());

    createBucketAndGenerateKeys(volume1, FILE_SYSTEM_OPTIMIZED, "fsobucket2");
    createBucketAndGenerateKeys(volume1, OBJECT_STORE, "obsbucket1");
    createBucketAndGenerateKeys(volume1, OBJECT_STORE, "obsbucket2");
    createBucketAndGenerateKeys(volume1, LEGACY, "legacybucket1");
    createBucketAndGenerateKeys(volume1, LEGACY, "legacybucket2");

    // Try volume delete without recursive
    // It should fail as volume is not empty
    final String[] args1 = new String[] {"volume", "delete", volume1};
    ExecutionException exception = assertThrows(ExecutionException.class,
        () -> execute(ozoneShell, args1));
    OMException omExecution = (OMException) exception.getCause();
    assertEquals(VOLUME_NOT_EMPTY, omExecution.getResult());
    out.reset();

    // volume1 should still exist
    assertEquals(client.getObjectStore().getVolume(volume1)
        .getName(), volume1);

    // Delete volume1(containing OBS, FSO and Legacy buckets) recursively with thread count
    String[] args = new String[] {"volume", "delete", volume1, "-r", "--yes", "-t", String.valueOf(threadCount)};

    execute(ozoneShell, args);
    out.reset();
    // volume1 should not exist
    omExecution = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume1));
    assertEquals(VOLUME_NOT_FOUND, omExecution.getResult());

    // volume2 should still exist
    assertEquals(client.getObjectStore().getVolume(volume2)
        .getName(), volume2);

    // Delete volume2 recursively
    args = new String[] {"volume", "delete", volume2, "-r", "--yes"};
    execute(ozoneShell, args);
    out.reset();

    // volume2 should not exist
    omExecution = assertThrows(OMException.class,
        () -> client.getObjectStore().getVolume(volume2));
    assertEquals(VOLUME_NOT_FOUND, omExecution.getResult());
  }

  private void createBucketAndGenerateKeys(String volume, BucketLayout layout, String bucketName) {
    // Create bucket
    String[] args = new String[] {"bucket", "create", volume + "/" +  bucketName,
        "--layout", layout.toString()};
    execute(ozoneShell, args);
    out.reset();

    // Insert keys
    String keyName = OZONE_URI_DELIMITER + volume + "/" + bucketName +
        OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 5; i++) {
      args = new String[] {
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
    out.reset();
  }

  @Test
  public void testLinkedAndNonLinkedBucketMetaData()
      throws Exception {
    String volumeName = "volume1";
    // Create volume volume1
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId +
          OZONE_URI_DELIMITER + volumeName};
    execute(ozoneShell, args);
    out.reset();

    // Create bucket bucket1
    args = new String[] {"bucket", "create", "o3://" + omServiceId +
          OZONE_URI_DELIMITER + volumeName + "/bucket1"};
    execute(ozoneShell, args);
    out.reset();

    // ozone sh bucket list
    out.reset();
    execute(ozoneShell, new String[] {"bucket", "list", "/volume1"});

    // Expect valid JSON array
    final List<Map<String, Object>> bucketListOut =
        parseOutputIntoArrayList();

    assertEquals(1, bucketListOut.size());
    boolean link =
        String.valueOf(bucketListOut.get(0).get("link")).equals("false");
    assertTrue(link);

    // Create linked bucket under volume1
    out.reset();
    execute(ozoneShell, new String[]{"bucket", "link", "/volume1/bucket1",
        "/volume1/link-to-bucket1"});

    // ozone sh bucket list under volume1 and this should give both linked
    // and non-linked buckets
    out.reset();
    execute(ozoneShell, new String[] {"bucket", "list", "/volume1"});

    // Expect valid JSON array
    final List<Map<String, Object>> bucketListLinked =
        parseOutputIntoArrayList();

    assertEquals(2, bucketListLinked.size());
    link = String.valueOf(bucketListLinked.get(1).get("link")).equals("true");
    assertTrue(link);

    // Clean up
    out.reset();
    execute(ozoneShell, new String[] {"bucket", "delete", "/volume1/bucket1"});
    out.reset();
    execute(ozoneShell,
        new String[]{"bucket", "delete", "/volume1/link-to-bucket1"});
    out.reset();
    execute(ozoneShell,
        new String[]{"volume", "delete", "/volume1"});
    out.reset();
  }

  @Test
  public void testKeyDeleteLegacyWithEnableFileSystemPath() throws IOException {
    String volumeName = "vol5";
    String bucketName = "legacybucket";
    String[] args = new String[] {"volume", "create", "o3://" + omServiceId + OZONE_URI_DELIMITER + volumeName};
    execute(ozoneShell, args);

    args = new String[] {"bucket", "create", "o3://" + omServiceId + OZONE_URI_DELIMITER +
          volumeName + OZONE_URI_DELIMITER + bucketName, "--layout", BucketLayout.LEGACY.toString()};
    execute(ozoneShell, args);

    String dirPath = OZONE_URI_DELIMITER + volumeName + OZONE_URI_DELIMITER +
        bucketName + OZONE_URI_DELIMITER + "dir/";
    String keyPath = dirPath + "key1";

    // Create key, it will generate two keys, one with dirPath other with keyPath
    args = new String[] {"key", "put", "o3://" + omServiceId + keyPath, testFile.getPath()};
    execute(ozoneShell, args);

    // Enable fileSystem path for client config
    String fileSystemEnable = generateSetConfString(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, "true");
    // Delete dirPath key, it should fail
    args = new String[] {fileSystemEnable, "key", "delete", dirPath};
    execute(ozoneShell, args);

    // Check number of keys
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    List<OzoneFileStatus> files = bucket.listStatus("", true, "", 5);
    // Two keys should still exist, dirPath and keyPath
    assertEquals(2, files.size());

    // cleanup
    args = new String[] {"volume", "delete", volumeName, "-r", "--yes"};
    execute(ozoneShell, args);
  }

  /**
   * Test that even though prepare commands are deprecated, they still succeed without blocking writes.
   */
  @Test
  public void testPrepareCommandNoOp() throws IOException {
    String volumeName = "vol-prepare-test";
    String bucketName = "bucket-prepare-test";
    
    // Execute prepare command via CLI
    String[] args = new String[] {"om", "prepare", "--service-id", omServiceId};
    execute(ozoneAdminShell, args);
    out.reset();
    
    // Verify write operations still work (prepare should not block them)
    // Create volume
    args = new String[] {"volume", "create", "o3://" + omServiceId + OZONE_URI_DELIMITER + volumeName};
    execute(ozoneShell, args);
    out.reset();
    
    // Create bucket
    args = new String[] {"bucket", "create", "o3://" + omServiceId + OZONE_URI_DELIMITER + volumeName +
            OZONE_URI_DELIMITER + bucketName};
    execute(ozoneShell, args);
    out.reset();
    
    // Create key
    String keyName = OZONE_URI_DELIMITER + volumeName + OZONE_URI_DELIMITER + 
        bucketName + OZONE_URI_DELIMITER + "testkey";
    args = new String[] {"key", "put", "o3://" + omServiceId + keyName, testFile.getPath()};
    execute(ozoneShell, args);
    out.reset();
    
    // Verify the key was created successfully
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull(bucket.getKey("testkey"), "Key should be created successfully after prepare");
    
    // Execute cancelprepare command via CLI
    args = new String[] {"om", "cancelprepare", "--service-id", omServiceId};
    execute(ozoneAdminShell, args);
    out.reset();
    
    // Cleanup
    args = new String[] {"volume", "delete", volumeName, "-r", "--yes"};
    execute(ozoneShell, args);
    out.reset();
  }

  private static String getKeyProviderURI(MiniKMS kms) {
    if (kms == null) {
      return "";
    }
    return KMSClientProvider.SCHEME_NAME + "://" +
        kms.getKMSUrl().toExternalForm().replace("://", "@");
  }

  protected MiniOzoneHAClusterImpl getCluster() {
    return cluster;
  }

  protected OzoneShell getOzoneShell() {
    return ozoneShell;
  }
}
