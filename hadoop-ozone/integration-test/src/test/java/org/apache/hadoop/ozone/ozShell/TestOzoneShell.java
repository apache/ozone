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
package org.apache.hadoop.ozone.ozShell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLRights;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Strings;
import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

/**
 * This test class specified for testing Ozone shell command.
 */
@RunWith(value = Parameterized.class)
public class TestOzoneShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShell.class);

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static String url;
  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  @SuppressWarnings("visibilitymodifier")
  public Class clientProtocol;
  /**
   * Create a MiniDFSCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestOzoneShell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    shell = new Shell();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    conf.setInt(OZONE_REPLICATION, ReplicationFactor.THREE.getValue());
    conf.setQuietMode(false);
    client = new RpcClient(conf);
    cluster.waitForClusterToBeReady();
  }

  /**
   * shutdown MiniDFSCluster.
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
  public void setup() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
    if(clientProtocol.equals(RestClient.class)) {
      String hostName = cluster.getOzoneManager().getHttpServer()
          .getHttpAddress().getHostName();
      int port = cluster
          .getOzoneManager().getHttpServer().getHttpAddress().getPort();
      url = String.format("http://" + hostName + ":" + port);
    } else {
      List<ServiceInfo> services = null;
      try {
        services = cluster.getOzoneManager().getServiceList();
      } catch (IOException e) {
        LOG.error("Could not get service list from OM");
      }
      String hostName = services.stream().filter(
          a -> a.getNodeType().equals(HddsProtos.NodeType.OM))
          .collect(Collectors.toList()).get(0).getHostname();

      String port = cluster.getOzoneManager().getRpcPort();
      url = String.format("o3://" + hostName + ":" + port);
    }
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

  @Test
  public void testCreateVolume() throws Exception {
    LOG.info("Running testCreateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume(volumeName, "");
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume("/////" + volumeName, "");
    testCreateVolume("/////", "Volume name is required");
    testCreateVolume("/////vol/123",
        "Invalid volume name. Delimiters (/) not allowed in volume name");
  }

  private void testCreateVolume(String volumeName, String errorMsg)
      throws Exception {
    err.reset();
    String userName = "bilbo";
    String[] args = new String[] {"volume", "create", url + "/" + volumeName,
        "--user", userName, "--root"};

    if (Strings.isNullOrEmpty(errorMsg)) {
      execute(shell, args);

    } else {
      executeWithError(shell, args, errorMsg);
      return;
    }

    String truncatedVolumeName =
        volumeName.substring(volumeName.lastIndexOf('/') + 1);
    OzoneVolume volumeInfo = client.getVolumeDetails(truncatedVolumeName);
    assertEquals(truncatedVolumeName, volumeInfo.getName());
    assertEquals(userName, volumeInfo.getOwner());
  }

  private void execute(Shell ozoneShell, String[] args) {
    List<String> arguments = new ArrayList(Arrays.asList(args));
    LOG.info("Executing shell command with args {}", arguments);
    CommandLine cmd = ozoneShell.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseResult) {
            throw ex;
          }
        };
    cmd.parseWithHandlers(new RunLast(),
        exceptionHandler, args);
  }

  /**
   * Test to create volume without specifying --user or -u.
   * @throws Exception
   */
  @Test
  public void testCreateVolumeWithoutUser() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"volume", "create", url + "/" + volumeName,
        "--root"};

    execute(shell, args);

    String truncatedVolumeName =
        volumeName.substring(volumeName.lastIndexOf('/') + 1);
    OzoneVolume volumeInfo = client.getVolumeDetails(truncatedVolumeName);
    assertEquals(truncatedVolumeName, volumeInfo.getName());
    assertEquals(UserGroupInformation.getCurrentUser().getUserName(),
        volumeInfo.getOwner());
  }

  @Test
  public void testDeleteVolume() throws Exception {
    LOG.info("Running testDeleteVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = client.getVolumeDetails(volumeName);
    assertNotNull(volume);

    String[] args = new String[] {"volume", "delete", url + "/" + volumeName};
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains("Volume " + volumeName + " is deleted"));

    // verify if volume has been deleted
    try {
      client.getVolumeDetails(volumeName);
      fail("Get volume call should have thrown.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Volume failed, error:VOLUME_NOT_FOUND", e);
    }


    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    volume = client.getVolumeDetails(volumeName);
    assertNotNull(volume);

    //volumeName prefixed with /
    String volumeNameWithSlashPrefix = "/" + volumeName;
    args = new String[] {"volume", "delete",
        url + "/" + volumeNameWithSlashPrefix};
    execute(shell, args);
    output = out.toString();
    assertTrue(output.contains("Volume " + volumeName + " is deleted"));

    // verify if volume has been deleted
    try {
      client.getVolumeDetails(volumeName);
      fail("Get volume call should have thrown.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Volume failed, error:VOLUME_NOT_FOUND", e);
    }
  }

  @Test
  public void testInfoVolume() throws Exception {
    LOG.info("Running testInfoVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);

    //volumeName supplied as-is
    String[] args = new String[] {"volume", "info", url + "/" + volumeName};
    execute(shell, args);

    String output = out.toString();
    assertTrue(output.contains(volumeName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    //volumeName prefixed with /
    String volumeNameWithSlashPrefix = "/" + volumeName;
    args = new String[] {"volume", "info",
        url + "/" + volumeNameWithSlashPrefix};
    execute(shell, args);

    output = out.toString();
    assertTrue(output.contains(volumeName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test infoVolume with invalid volume name
    args = new String[] {"volume", "info",
        url + "/" + volumeName + "/invalid-name"};
    executeWithError(shell, args, "Invalid volume name. " +
        "Delimiters (/) not allowed in volume name");

    // get info for non-exist volume
    args = new String[] {"volume", "info", url + "/invalid-volume"};
    executeWithError(shell, args, "VOLUME_NOT_FOUND");
  }

  @Test
  public void testShellIncompleteCommand() throws Exception {
    LOG.info("Running testShellIncompleteCommand");
    String expectedError = "Incomplete command";
    String[] args = new String[] {}; //executing 'ozone sh'

    executeWithError(shell, args, expectedError,
        "Usage: ozone sh [-hV] [--verbose] [-D=<String=String>]..." +
            " [COMMAND]");

    args = new String[] {"volume"}; //executing 'ozone sh volume'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh volume [-hV] [COMMAND]");

    args = new String[] {"bucket"}; //executing 'ozone sh bucket'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh bucket [-hV] [COMMAND]");

    args = new String[] {"key"}; //executing 'ozone sh key'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh key [-hV] [COMMAND]");
  }

  @Test
  public void testUpdateVolume() throws Exception {
    LOG.info("Running testUpdateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(), vol.getQuota());

    String[] args = new String[] {"volume", "update", url + "/" + volumeName,
        "--quota", "500MB"};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("500MB").sizeInBytes(), vol.getQuota());

    String newUser = "new-user";
    args = new String[] {"volume", "update", url + "/" + volumeName,
        "--user", newUser};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(newUser, vol.getOwner());

    //volume with / prefix
    String volumeWithPrefix = "/" + volumeName;
    String newUser2 = "new-user2";
    args = new String[] {"volume", "update", url + "/" + volumeWithPrefix,
        "--user", newUser2};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(newUser2, vol.getOwner());

    // test error conditions
    args = new String[] {"volume", "update", url + "/invalid-volume",
        "--user", newUser};
    executeWithError(shell, args, "Info Volume failed, error:VOLUME_NOT_FOUND");

    err.reset();
    args = new String[] {"volume", "update", url + "/invalid-volume",
        "--quota", "500MB"};
    executeWithError(shell, args, "Info Volume failed, error:VOLUME_NOT_FOUND");
  }

  /**
   * Execute command, assert exeception message and returns true if error
   * was thrown.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(ozoneShell, args);
    } else {
      try {
        execute(ozoneShell, args);
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
                  "Error of shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown and contains the specified usage string.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      String expectedError, String usage) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(ozoneShell, args);
    } else {
      try {
        execute(ozoneShell, args);
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
                  "Error of shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
          Assert.assertTrue(
              exceptionToCheck instanceof MissingSubcommandException);
          Assert.assertTrue(
              ((MissingSubcommandException)exceptionToCheck)
                  .getUsage().contains(usage));
        }
      }
    }
  }

  @Test
  public void testListVolume() throws Exception {
    LOG.info("Running testListVolume");
    String protocol = clientProtocol.getName().toLowerCase();
    String commandOutput, commandError;
    List<VolumeInfo> volumes;
    final int volCount = 20;
    final String user1 = "test-user-a-" + protocol;
    final String user2 = "test-user-b-" + protocol;

    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol-" + protocol + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol-" + protocol + x;
      }
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .build();
      client.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = client.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }

    String[] args = new String[] {"volume", "list", url + "/abcde", "--user",
        user1, "--length", "100"};
    executeWithError(shell, args, "Invalid URI");

    err.reset();
    // test -length option
    args = new String[] {"volume", "list", url + "/", "--user",
        user1, "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(10, volumes.size());
    for (VolumeInfo volume : volumes) {
      assertEquals(volume.getOwner().getName(), user1);
      assertTrue(volume.getCreatedOn().contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user1, "--length", "2"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    // test --prefix option
    out.reset();
    args =
        new String[] {"volume", "list", url + "/", "--user", user1, "--length",
            "100", "--prefix", "test-vol-" + protocol + "1"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(5, volumes.size());
    // return volume names should be [test-vol10, test-vol12, ..., test-vol18]
    for (int i = 0; i < volumes.size(); i++) {
      assertEquals(volumes.get(i).getVolumeName(),
          "test-vol-" + protocol + ((i + 5) * 2));
      assertEquals(volumes.get(i).getOwner().getName(), user1);
    }

    // test -start option
    out.reset();
    args =
        new String[] {"volume", "list", url + "/", "--user", user2, "--length",
            "100", "--start", "test-vol-" + protocol + "15"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    assertEquals(volumes.get(0).getVolumeName(), "test-vol-" + protocol + "17");
    assertEquals(volumes.get(1).getVolumeName(), "test-vol-" + protocol + "19");
    assertEquals(volumes.get(0).getOwner().getName(), user2);
    assertEquals(volumes.get(1).getOwner().getName(), user2);

    // test error conditions
    err.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user2, "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");

    err.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user2, "--length", "invalid-length"};
    executeWithError(shell, args, "For input string: \"invalid-length\"");
  }

  @Test
  public void testCreateBucket() throws Exception {
    LOG.info("Running testCreateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"bucket", "create",
        url + "/" + vol.getName() + "/" + bucketName};

    execute(shell, args);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertEquals(vol.getName(),
        bucketInfo.getVolumeName());
    assertEquals(bucketName, bucketInfo.getName());

    // test create a bucket in a non-exist volume
    args = new String[] {"bucket", "create",
        url + "/invalid-volume/" + bucketName};
    executeWithError(shell, args, "Info Volume failed, error:VOLUME_NOT_FOUND");

    // test createBucket with invalid bucket name
    args = new String[] {"bucket", "create",
        url + "/" + vol.getName() + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args,
        "Invalid bucket name. Delimiters (/) not allowed in bucket name");
  }

  @Test
  public void testDeleteBucket() throws Exception {
    LOG.info("Running testDeleteBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertNotNull(bucketInfo);

    String[] args = new String[] {"bucket", "delete",
        url + "/" + vol.getName() + "/" + bucketName};
    execute(shell, args);

    // verify if bucket has been deleted in volume
    try {
      vol.getBucket(bucketName);
      fail("Get bucket should have thrown.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Bucket failed, error: BUCKET_NOT_FOUND", e);
    }

    // test delete bucket in a non-exist volume
    args = new String[] {"bucket", "delete",
        url + "/invalid-volume" + "/" + bucketName};
    executeWithError(shell, args, "Info Volume failed, error:VOLUME_NOT_FOUND");

    err.reset();
    // test delete non-exist bucket
    args = new String[] {"bucket", "delete",
        url + "/" + vol.getName() + "/invalid-bucket"};
    executeWithError(shell, args,
        "Delete Bucket failed, error:BUCKET_NOT_FOUND");
  }

  @Test
  public void testInfoBucket() throws Exception {
    LOG.info("Running testInfoBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);

    String[] args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/" + bucketName};
    execute(shell, args);

    String output = out.toString();
    assertTrue(output.contains(bucketName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test infoBucket with invalid bucket name
    args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args,
        "Invalid bucket name. Delimiters (/) not allowed in bucket name");

    // test get info from a non-exist bucket
    args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/invalid-bucket" + bucketName};
    executeWithError(shell, args,
        "Info Bucket failed, error: BUCKET_NOT_FOUND");
  }

  @Test
  public void testUpdateBucket() throws Exception {
    LOG.info("Running testUpdateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    int aclSize = bucket.getAcls().size();

    String[] args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/" + bucketName, "--addAcl",
        "user:frodo:rw,group:samwise:r"};
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    bucket = vol.getBucket(bucketName);
    assertEquals(2 + aclSize, bucket.getAcls().size());

    OzoneAcl acl = bucket.getAcls().get(aclSize);
    assertTrue(acl.getName().equals("frodo")
        && acl.getType() == OzoneACLType.USER
        && acl.getRights()== OzoneACLRights.READ_WRITE);

    args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/" + bucketName, "--removeAcl",
        "user:frodo:rw"};
    execute(shell, args);

    bucket = vol.getBucket(bucketName);
    acl = bucket.getAcls().get(aclSize);
    assertEquals(1 + aclSize, bucket.getAcls().size());
    assertTrue(acl.getName().equals("samwise")
        && acl.getType() == OzoneACLType.GROUP
        && acl.getRights()== OzoneACLRights.READ);

    // test update bucket for a non-exist bucket
    args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/invalid-bucket", "--addAcl",
        "user:frodo:rw"};
    executeWithError(shell, args,
        "Info Bucket failed, error: BUCKET_NOT_FOUND");
  }

  @Test
  public void testListBucket() throws Exception {
    LOG.info("Running testListBucket");
    List<BucketInfo> buckets;
    String commandOutput;
    int bucketCount = 11;
    OzoneVolume vol = creatVolume();

    List<String> bucketNames = new ArrayList<>();
    // create bucket from test-bucket0 to test-bucket10
    for (int i = 0; i < bucketCount; i++) {
      String name = "test-bucket" + i;
      bucketNames.add(name);
      vol.createBucket(name);
      OzoneBucket bucket = vol.getBucket(name);
      assertNotNull(bucket);
    }

    // test listBucket with invalid volume name
    String[] args = new String[] {"bucket", "list",
        url + "/" + vol.getName() + "/invalid-name"};
    executeWithError(shell, args, "Invalid volume name. " +
        "Delimiters (/) not allowed in volume name");

    // test -length option
    args = new String[] {"bucket", "list",
        url + "/" + vol.getName(), "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(11, buckets.size());
    // sort bucket names since the return buckets isn't in created order
    Collections.sort(bucketNames);
    // return bucket names should be [test-bucket0, test-bucket1,
    // test-bucket10, test-bucket2, ,..., test-bucket9]
    for (int i = 0; i < buckets.size(); i++) {
      assertEquals(buckets.get(i).getBucketName(), bucketNames.get(i));
      assertEquals(buckets.get(i).getVolumeName(), vol.getName());
      assertTrue(buckets.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "3"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(3, buckets.size());
    // return bucket names should be [test-bucket0,
    // test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket0");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(2).getBucketName(), "test-bucket10");

    // test --prefix option
    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "100", "--prefix", "test-bucket1"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    // return bucket names should be [test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket10");

    // test -start option
    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "100", "--start", "test-bucket7"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    assertEquals(buckets.get(0).getBucketName(), "test-bucket8");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket9");

    // test error conditions
    err.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");
  }

  @Test
  public void testPutKey() throws Exception {
    LOG.info("Running testPutKey");
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    String[] args = new String[] {"key", "put",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        createTmpFile()};
    execute(shell, args);

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    // test put key in a non-exist bucket
    args = new String[] {"key", "put",
        url + "/" + volumeName + "/invalid-bucket/" + keyName,
        createTmpFile()};
    executeWithError(shell, args,
        "Info Bucket failed, error: BUCKET_NOT_FOUND");
  }

  @Test
  public void testGetKey() throws Exception {
    LOG.info("Running testGetKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    String tmpPath = baseDir.getAbsolutePath() + "/testfile-"
        + UUID.randomUUID().toString();
    String[] args = new String[] {"key", "get",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        tmpPath};
    execute(shell, args);

    byte[] dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));

    tmpPath = baseDir.getAbsolutePath() + File.separatorChar + keyName;
    args = new String[] {"key", "get",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        baseDir.getAbsolutePath()};
    execute(shell, args);

    dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));
  }

  @Test
  public void testDeleteKey() throws Exception {
    LOG.info("Running testDeleteKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    String[] args = new String[] {"key", "delete",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};
    execute(shell, args);

    // verify if key has been deleted in the bucket
    try {
      bucket.getKey(keyName);
      fail("Get key should have thrown.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Lookup key failed, error:KEY_NOT_FOUND", e);
    }

    // test delete key in a non-exist bucket
    args = new String[] {"key", "delete",
        url + "/" + volumeName + "/invalid-bucket/" + keyName};
    executeWithError(shell, args,
        "Info Bucket failed, error: BUCKET_NOT_FOUND");

    err.reset();
    // test delete a non-exist key in bucket
    args = new String[] {"key", "delete",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};
    executeWithError(shell, args, "Delete key failed, error:KEY_NOT_FOUND");
  }

  @Test
  public void testInfoKeyDetails() throws Exception {
    LOG.info("Running testInfoKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    String[] args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};

    // verify the response output
    execute(shell, args);
    String output = out.toString();

    assertTrue(output.contains(keyName));
    assertTrue(
        output.contains("createdOn") && output.contains("modifiedOn") && output
            .contains(OzoneConsts.OZONE_TIME_ZONE));
    assertTrue(
        output.contains("containerID") && output.contains("localID") && output
            .contains("length") && output.contains("offset"));
    // reset stream
    out.reset();
    err.reset();

    // get the info of a non-exist key
    args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};

    // verify the response output
    // get the non-exist key info should be failed
    executeWithError(shell, args, "Lookup key failed, error:KEY_NOT_FOUND");
  }

  @Test
  public void testInfoDirKey() throws Exception {
    LOG.info("Running testInfoKey for Dir Key");
    String dirKeyName = "test/";
    String keyNameOnly = "test";
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(dirKeyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();
    String[] args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + dirKeyName};
    // verify the response output
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains(dirKeyName));
    assertTrue(output.contains("createdOn") &&
                output.contains("modifiedOn") &&
                output.contains(OzoneConsts.OZONE_TIME_ZONE));
    args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + keyNameOnly};
    executeWithError(shell, args, "Lookup key failed, error:KEY_NOT_FOUND");
    out.reset();
    err.reset();
  }

  @Test
  public void testListKey() throws Exception {
    LOG.info("Running testListKey");
    String commandOutput;
    List<KeyInfo> keys;
    int keyCount = 11;
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String keyName;
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      keyName = "test-key" + i;
      keyNames.add(keyName);
      String dataStr = "test-data";
      OzoneOutputStream keyOutputStream =
          bucket.createKey(keyName, dataStr.length());
      keyOutputStream.write(dataStr.getBytes());
      keyOutputStream.close();
    }

    // test listKey with invalid bucket name
    String[] args = new String[] {"key", "list",
        url + "/" + volumeName + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args, "Invalid bucket name. " +
        "Delimiters (/) not allowed in bucket name");

    // test -length option
    args = new String[] {"key", "list",
        url + "/" + volumeName + "/" + bucketName, "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(11, keys.size());
    // sort key names since the return keys isn't in created order
    Collections.sort(keyNames);
    // return key names should be [test-key0, test-key1,
    // test-key10, test-key2, ,..., test-key9]
    for (int i = 0; i < keys.size(); i++) {
      assertEquals(keys.get(i).getKeyName(), keyNames.get(i));
      // verify the creation/modification time of key
      assertTrue(keys.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
      assertTrue(keys.get(i).getModifiedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "3"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(3, keys.size());
    // return key names should be [test-key0, test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key0");
    assertEquals(keys.get(1).getKeyName(), "test-key1");
    assertEquals(keys.get(2).getKeyName(), "test-key10");

    // test --prefix option
    out.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "100", "--prefix", "test-key1"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(2, keys.size());
    // return key names should be [test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key1");
    assertEquals(keys.get(1).getKeyName(), "test-key10");

    // test -start option
    out.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "100", "--start", "test-key7"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(keys.get(0).getKeyName(), "test-key8");
    assertEquals(keys.get(1).getKeyName(), "test-key9");

    // test error conditions
    err.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");
  }

  @Test
  public void testS3BucketMapping() throws  IOException {

    List<ServiceInfo> services =
        cluster.getOzoneManager().getServiceList();

    String omHostName = services.stream().filter(
        a -> a.getNodeType().equals(HddsProtos.NodeType.OM))
        .collect(Collectors.toList()).get(0).getHostname();

    String omPort = cluster.getOzoneManager().getRpcPort();
    String setOmAddress =
        "--set=" + OZONE_OM_ADDRESS_KEY + "=" + omHostName + ":" + omPort;

    String s3Bucket = "bucket1";
    String commandOutput;
    createS3Bucket("ozone", s3Bucket);

    //WHEN
    String[] args =
        new String[] {setOmAddress, "bucket",
            "path", s3Bucket};
    execute(shell, args);

    //THEN
    commandOutput = out.toString();
    String volumeName = client.getOzoneVolumeName(s3Bucket);
    assertTrue(commandOutput.contains("Volume name for S3Bucket is : " +
        volumeName));
    assertTrue(commandOutput.contains(OzoneConsts.OZONE_URI_SCHEME + "://" +
        s3Bucket + "." + volumeName));
    out.reset();

    //Trying to get map for an unknown bucket
    args = new String[] {setOmAddress, "bucket", "path",
        "unknownbucket"};
    executeWithError(shell, args, "S3_BUCKET_NOT_FOUND");

    // No bucket name
    args = new String[] {setOmAddress, "bucket", "path"};
    executeWithError(shell, args, "Missing required parameter");

    // Invalid bucket name
    args = new String[] {setOmAddress, "bucket", "path", "/asd/multipleslash"};
    executeWithError(shell, args, "S3_BUCKET_NOT_FOUND");
  }

  @Test
  public void testS3Secret() throws Exception {
    List<ServiceInfo> services =
        cluster.getOzoneManager().getServiceList();

    String omHostName = services.stream().filter(
        a -> a.getNodeType().equals(HddsProtos.NodeType.OM))
        .collect(Collectors.toList()).get(0).getHostname();

    String omPort = cluster.getOzoneManager().getRpcPort();
    String setOmAddress =
        "--set=" + OZONE_OM_ADDRESS_KEY + "=" + omHostName + ":" + omPort;

    err.reset();
    String outputFirstAttempt;
    String outputSecondAttempt;

    //First attempt: If secrets are not found in database, they will be created
    String[] args = new String[] {setOmAddress, "s3", "getsecret"};
    execute(shell, args);
    outputFirstAttempt = out.toString();
    //Extracting awsAccessKey & awsSecret value from output
    String[] output = outputFirstAttempt.split("\n");
    String awsAccessKey = output[0].split("=")[1];
    String awsSecret = output[1].split("=")[1];
    assertTrue((awsAccessKey != null && awsAccessKey.length() > 0) &&
            (awsSecret != null && awsSecret.length() > 0));

    out.reset();

    //Second attempt: Since secrets were created in previous attempt, it
    // should return the same value
    args = new String[] {setOmAddress, "s3", "getsecret"};
    execute(shell, args);
    outputSecondAttempt = out.toString();

    //verifying if secrets from both attempts are same
    assertTrue(outputFirstAttempt.equals(outputSecondAttempt));
  }

  private void createS3Bucket(String userName, String s3Bucket) {
    try {
      client.createS3Bucket("ozone", s3Bucket);
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_ALREADY_EXISTS", ex);
    }
  }

  private OzoneVolume creatVolume() throws OzoneException, IOException {
    String volumeName = RandomStringUtils.randomNumeric(5) + "volume";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    try {
      client.createVolume(volumeName, volumeArgs);
    } catch (Exception ex) {
      Assert.assertEquals("PartialGroupNameException",
          ex.getCause().getClass().getSimpleName());
    }
    OzoneVolume volume = client.getVolumeDetails(volumeName);

    return volume;
  }

  private OzoneBucket creatBucket() throws OzoneException, IOException {
    OzoneVolume vol = creatVolume();
    String bucketName = RandomStringUtils.randomNumeric(5) + "bucket";
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);

    return bucketInfo;
  }

  /**
   * Create a temporary file used for putting key.
   * @return the created file's path string
   * @throws Exception
   */
  private String createTmpFile() throws Exception {
    // write a new file that used for putting key
    File tmpFile = new File(baseDir,
        "/testfile-" + UUID.randomUUID().toString());
    FileOutputStream randFile = new FileOutputStream(tmpFile);
    Random r = new Random();
    for (int x = 0; x < 10; x++) {
      char c = (char) (r.nextInt(26) + 'a');
      randFile.write(c);
    }
    randFile.close();

    return tmpFile.getAbsolutePath();
  }
}
