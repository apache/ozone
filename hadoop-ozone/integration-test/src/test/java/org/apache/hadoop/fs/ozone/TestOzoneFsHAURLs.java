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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getHostPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Optional;
import java.util.OptionalInt;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.HATests;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test client-side URI handling with Ozone Manager HA.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFsHAURLs implements HATests.TestCase {

  /**
    * Set a timeout for each test.
    */
  private static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneFsHAURLs.class);

  private OzoneConfiguration conf;
  private MiniOzoneHAClusterImpl cluster;
  private String omServiceId;
  private OzoneManager om;

  private String volumeName;
  private String bucketName;
  private String rootPath;

  private static final String O3FS_IMPL_KEY =
      "fs." + OzoneConsts.OZONE_URI_SCHEME + ".impl";
  private static final String O3FS_IMPL_VALUE =
      "org.apache.hadoop.fs.ozone.OzoneFileSystem";
  private OzoneClient client;

  private static final String OFS_IMPL_KEY =
      "fs." + OzoneConsts.OZONE_OFS_URI_SCHEME + ".impl";

  private static final String OFS_IMPL_VALUE =
      "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem";

  @BeforeAll
  void initClass() throws Exception {
    cluster = cluster();
    omServiceId = cluster.getOzoneManager().getOMServiceId();
    client = cluster.newClient();

    om = cluster.getOzoneManager();
  }

  @BeforeEach
  public void init() throws Exception {
    // Duplicate the conf for each test, so the client can change it, and each
    // test will still get the same base conf used to start the cluster.
    conf = new OzoneConfiguration(cluster.getConf());

    assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());

    volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);
    bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
    retVolumeinfo.createBucket(bucketName);

    rootPath = String.format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
        bucketName, volumeName, omServiceId);
    // Set fs.defaultFS
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);
    // Create some dirs
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  /**
   * @return the leader OM's RPC address in the MiniOzoneHACluster
   */
  private String getLeaderOMNodeAddr() {
    OzoneManager omLeader = cluster.getOMLeader();
    assertNotNull(omLeader, "There should be a leader OM at this point.");
    String omNodeId = omLeader.getOMNodeId();
    // omLeaderAddrKey=ozone.om.address.omServiceId.omNodeId
    String omLeaderAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
    String omLeaderAddr = conf.get(omLeaderAddrKey);
    LOG.info("OM leader: nodeId={}, {}={}", omNodeId, omLeaderAddrKey,
            omLeaderAddr);
    return omLeaderAddr;
  }

  /**
   * Get host name from an address. This uses getHostName() internally.
   * @param addr Address with port number
   * @return Host name
   */
  private String getHostFromAddress(String addr) {
    Optional<String> hostOptional = getHostName(addr);
    assert (hostOptional.isPresent());
    return hostOptional.get();
  }

  /**
   * Get port number from an address. This uses getHostPort() internally.
   * @param addr Address with port
   * @return Port number
   */
  private int getPortFromAddress(String addr) {
    OptionalInt portOptional = getHostPort(addr);
    assert (portOptional.isPresent());
    return portOptional.getAsInt();
  }

  /**
   * Test OM HA URLs with qualified fs.defaultFS.
   * @throws Exception
   */
  @Test
  public void testWithQualifiedDefaultFS() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setQuietMode(false);
    clientConf.set(O3FS_IMPL_KEY, O3FS_IMPL_VALUE);
    // fs.defaultFS = o3fs://bucketName.volumeName.omServiceId/
    clientConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    // Pick leader OM's RPC address and assign it to ozone.om.address for
    // the test case: ozone fs -ls o3fs://bucket.volume.om1/
    String leaderOMNodeAddr = getLeaderOMNodeAddr();
    // ozone.om.address was set to service id in MiniOzoneHAClusterImpl
    clientConf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, leaderOMNodeAddr);

    FsShell shell = new FsShell(clientConf);
    int res;
    try {
      // Test case 1: ozone fs -ls /
      // Expectation: Success.
      res = ToolRunner.run(shell, new String[] {"-ls", "/"});
      // Check return value, should be 0 (success)
      assertEquals(0, res);

      // Test case 2: ozone fs -ls o3fs:///
      // Expectation: Success. fs.defaultFS is a fully qualified path.
      res = ToolRunner.run(shell, new String[] {"-ls", "o3fs:///"});
      assertEquals(0, res);

      // Test case 3: ozone fs -ls o3fs://bucket.volume/
      // Expectation: Fail. Must have service id or host name when HA is enabled
      String unqualifiedPath1 = String.format("%s://%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] {"-ls", unqualifiedPath1});
        // Check stderr, inspired by testDFSWithInvalidCommmand
        assertThat(capture.getOutput())
            .as("ozone fs -ls o3fs://bucket.volume/")
            .contains("-ls: Service ID or host name must not be omitted when ozone.om.service.ids is defined.");
      }
      // Check return value, should be -1 (failure)
      assertEquals(-1, res);

      // Test case 4: ozone fs -ls o3fs://bucket.volume.om1/
      // Expectation: Success. The client should use the port number
      // set in ozone.om.address.
      String qualifiedPath1 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          getHostFromAddress(leaderOMNodeAddr));
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath1});
      // Note: this test case will fail if the port is not from the leader node
      assertEquals(0, res);

      // Test case 5: ozone fs -ls o3fs://bucket.volume.om1:port/
      // Expectation: Success.
      String qualifiedPath2 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          leaderOMNodeAddr);
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath2});
      assertEquals(0, res);

      // Test case 6: ozone fs -ls o3fs://bucket.volume.id1/
      // Expectation: Success.
      String qualifiedPath3 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName, omServiceId);
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath3});
      assertEquals(0, res);

      // Test case 7: ozone fs -ls o3fs://bucket.volume.id1:port/
      // Expectation: Fail. Service ID does not use port information.
      // Use the port number from leader OM (doesn't really matter)
      String unqualifiedPath2 = String.format("%s://%s.%s.%s:%d/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          omServiceId, getPortFromAddress(leaderOMNodeAddr));
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] {"-ls", unqualifiedPath2});
        // Check stderr
        assertThat(capture.getOutput())
            .as("ozone fs -ls o3fs://bucket.volume.id1:port/")
            .contains("does not use port information");
      }
      // Check return value, should be -1 (failure)
      assertEquals(-1, res);
    } finally {
      shell.close();
    }
  }

  /**
   * Helper function for testOtherDefaultFS(),
   * run fs -ls o3fs:/// against different fs.defaultFS input.
   *
   * @param defaultFS Desired fs.defaultFS to be used in the test
   * @throws Exception
   */
  private void testWithDefaultFS(String defaultFS) throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setQuietMode(false);
    clientConf.set(O3FS_IMPL_KEY, O3FS_IMPL_VALUE);
    // fs.defaultFS = file:///
    clientConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        defaultFS);

    FsShell shell = new FsShell(clientConf);
    try {
      // Test case: ozone fs -ls o3fs:///
      // Expectation: Fail. fs.defaultFS is not a qualified o3fs URI.
      int res = ToolRunner.run(shell, new String[] {"-ls", "o3fs:///"});
      assertEquals(-1, res);
    } finally {
      shell.close();
    }
  }

  /**
   * Test OM HA URLs with some unqualified fs.defaultFS.
   * @throws Exception
   */
  @Test
  public void testOtherDefaultFS() throws Exception {
    // Test scenarios where fs.defaultFS isn't a fully qualified o3fs

    // fs.defaultFS = file:///
    testWithDefaultFS(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);

    // fs.defaultFS = hdfs://ns1/
    testWithDefaultFS("hdfs://ns1/");

    // fs.defaultFS = o3fs:///
    String unqualifiedFs1 = String.format(
        "%s:///", OzoneConsts.OZONE_URI_SCHEME);
    testWithDefaultFS(unqualifiedFs1);

    // fs.defaultFS = o3fs://bucketName.volumeName/
    String unqualifiedFs2 = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
    testWithDefaultFS(unqualifiedFs2);
  }

  @Test
  public void testIncorrectAuthorityInURI() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setQuietMode(false);
    clientConf.set(O3FS_IMPL_KEY, O3FS_IMPL_VALUE);
    clientConf.set(OFS_IMPL_KEY, OFS_IMPL_VALUE);
    FsShell shell = new FsShell(clientConf);
    String incorrectSvcId = "dummy";
    String o3fsPathWithCorrectSvcId =
        String.format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
            bucketName, volumeName, omServiceId);
    String o3fsPathWithInCorrectSvcId =
        String.format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
            bucketName, volumeName, incorrectSvcId);
    String ofsPathWithCorrectSvcId = "ofs://" + omServiceId + "/";
    String ofsPathWithIncorrectSvcId = "ofs://" + incorrectSvcId + "/";
    try {
      int res = ToolRunner.run(shell,
          new String[] {"-ls", ofsPathWithCorrectSvcId });
      assertEquals(0, res);
      res = ToolRunner.run(shell,
          new String[] {"-ls", o3fsPathWithCorrectSvcId });
      assertEquals(0, res);

      try (GenericTestUtils.SystemErrCapturer capture = new
          GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell,
            new String[] {"-ls", ofsPathWithIncorrectSvcId });
        assertEquals(1, res);
        assertThat(capture.getOutput()).contains("Cannot resolve OM host");
      }

      try (GenericTestUtils.SystemErrCapturer capture = new
          GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell,
            new String[] {"-ls", o3fsPathWithInCorrectSvcId });
        assertEquals(1, res);
        assertThat(capture.getOutput()).contains("Cannot resolve OM host");
      }
    } finally {
      shell.close();
    }

  }
}
