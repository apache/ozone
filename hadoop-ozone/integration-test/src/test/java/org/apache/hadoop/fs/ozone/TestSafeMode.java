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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterProvider;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestSafeMode {

  private static final String VOLUME = "vol";
  private static final String BUCKET = "bucket";
  private static MiniOzoneClusterProvider clusterProvider;

  private MiniOzoneCluster cluster;

  @BeforeAll
  static void setup() {
    OzoneConfiguration conf = new OzoneConfiguration();
    clusterProvider = new MiniOzoneClusterProvider(
        MiniOzoneCluster.newBuilder(conf), 2);
  }

  @BeforeEach
  void createCluster() throws Exception {
    cluster = clusterProvider.provide();
    cluster.waitForClusterToBeReady();
    try (OzoneClient client = cluster.newClient()) {
      client.getObjectStore().createVolume(VOLUME);
      client.getObjectStore().getVolume(VOLUME).createBucket(BUCKET);
    }
  }

  @AfterEach
  void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @AfterAll
  static void shutdown() throws Exception {
    clusterProvider.shutdown();
  }

  @Test
  void ofs() throws Exception {
    testSafeMode(TestSafeMode::getOFSRoot);
  }

  @Test
  void o3fs() throws Exception {
    testSafeMode(TestSafeMode::getO3FSRoot);
  }

  private void testSafeMode(Function<OzoneConfiguration, String> fsRoot)
      throws Exception {
    FileSystem fs = createFS(fsRoot);
    try {
      SafeMode subject = assertInstanceOf(SafeMode.class, fs);

      assertFalse(subject.setSafeMode(SafeModeAction.GET));

      cluster.shutdownHddsDatanodes();
      cluster.restartStorageContainerManager(false);

      // SCM should be in safe mode
      assertTrue(subject.setSafeMode(SafeModeAction.GET));

      // force exit safe mode and verify that it's out of safe mode.
      subject.setSafeMode(SafeModeAction.FORCE_EXIT);
      assertFalse(subject.setSafeMode(SafeModeAction.GET));

      // datanodes are still stopped
      RatisReplicationConfig replication =
          RatisReplicationConfig.getInstance(THREE);
      assertThrows(IOException.class, () -> cluster.getStorageContainerManager()
          .getWritableContainerFactory()
          .getContainer(MB, replication, OZONE, new ExcludeList()));
    } finally {
      IOUtils.closeQuietly(fs);
    }
  }

  private FileSystem createFS(Function<OzoneConfiguration, String> fsRoot)
      throws IOException {
    OzoneConfiguration conf = cluster.getConf();
    URI uri = URI.create(fsRoot.apply(conf));
    return FileSystem.get(uri, conf);
  }

  private static String getOFSRoot(OzoneConfiguration conf) {
    return String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));
  }

  private static String getO3FSRoot(OzoneConfiguration conf) {
    return String.format("%s://%s.%s.%s/",
        OZONE_URI_SCHEME, BUCKET, VOLUME, conf.get(OZONE_OM_ADDRESS_KEY));
  }

}
