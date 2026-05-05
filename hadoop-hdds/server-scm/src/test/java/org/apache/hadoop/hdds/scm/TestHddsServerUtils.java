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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HddsServerUtil}.
 */
public class TestHddsServerUtils {

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY with port.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testGetDatanodeAddressWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), Integer.parseInt(scmHost.split(":")[1]));
  }

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY without port.
   */
  @Test
  public void testGetDatanodeAddressWithoutPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY.
   */
  @Test
  public void testDatanodeAddressFallbackToClientNoPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY. Port number defined by
   * OZONE_SCM_CLIENT_ADDRESS_KEY should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToClientWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), OZONE_SCM_DATANODE_PORT_DEFAULT);
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES.
   */
  @Test
  public void testDatanodeAddressFallbackToScmNamesNoPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES. Port number
   * defined by OZONE_SCM_NAMES should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToScmNamesWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * Test {@link ServerUtils#getScmDbDir}.
   */
  @Test
  public void testGetScmDbDir() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File dbDir = new File(testDir, "scmDbDir");
    final File metaDir = new File(testDir, "metaDir");   // should be ignored.
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(dbDir, ServerUtils.getScmDbDir(conf));
      assertTrue(dbDir.exists());          // should have been created.
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link ServerUtils#getScmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_SCM_DB_DIRS is undefined.
   */
  @Test
  public void testGetScmDbDirWithFallback() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());
    try {
      assertEquals(metaDir, ServerUtils.getScmDbDir(conf));
      assertTrue(metaDir.exists());        // should have been created.
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoScmDbDirConfigured() {
    assertThrows(IllegalArgumentException.class,
        () -> ServerUtils.getScmDbDir(new OzoneConfiguration()));
  }

  @Test
  public void testGetStaleNodeInterval() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // Reset OZONE_SCM_STALENODE_INTERVAL to 300s that
    // larger than max limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 300, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the max limit value will be returned
    assertEquals(100000, HddsServerUtil.getStaleNodeInterval(conf));

    // Reset OZONE_SCM_STALENODE_INTERVAL to 10ms that
    // smaller than min limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 10,
        TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the min limit value will be returned
    assertEquals(90000, HddsServerUtil.getStaleNodeInterval(conf));
  }

  @Test
  public void testGetDatanodeIdFilePath() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      // test fallback if not set
      assertEquals(new File(metaDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));

      // test fallback if set empty
      conf.set(OZONE_SCM_DATANODE_ID_DIR, "");
      assertEquals(new File(metaDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));

      // test use specific value if set
      final File dnIdDir = new File(testDir, "datanodeIDDir");
      conf.set(OZONE_SCM_DATANODE_ID_DIR, dnIdDir.getPath());
      assertEquals(new File(dnIdDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }
}
