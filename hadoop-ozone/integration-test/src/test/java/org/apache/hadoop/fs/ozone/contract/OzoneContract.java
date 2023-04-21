/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.ozone.contract;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.Assert;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED;

/**
 * The contract of Ozone: only enabled if the test bucket is provided.
 */
class OzoneContract extends AbstractFSContract {

  private static final List<Boolean> FSO_COMBINATIONS = Arrays.asList(
      // FSO configuration is a cluster level server side configuration.
      // If the cluster is configured with SIMPLE metadata layout,
      // non-FSO bucket will created.
      // If the cluster is configured with PREFIX metadata layout,
      // FSO bucket will be created.
      // Presently, OzoneClient checks bucketMetadata then invokes FSO or
      // non-FSO specific code and it makes no sense to add client side
      // configs now. Once the specific client API to set FSO or non-FSO
      // bucket is provided the contract test can be refactored to include
      // another parameter (fsoClient) which sets/unsets the client side
      // configs.
      true, // Server is configured with new layout (PREFIX)
      // and new buckets will be operated on
      false // Server is configured with old layout (SIMPLE)
      // and old buckets will be operated on
  );
  private static MiniOzoneCluster cluster;
  private static final String CONTRACT_XML = "contract/ozone.xml";

  private static boolean fsOptimizedServer;
  private static OzoneClient client;

  OzoneContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  static List<Boolean> getFsoCombinations() {
    return FSO_COMBINATIONS;
  }

  @Override
  public String getScheme() {
    return OzoneConsts.OZONE_URI_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path("/test");
  }

  public static void initOzoneConfiguration(boolean fsoServer) {
    fsOptimizedServer = fsoServer;
  }

  public static void createCluster(boolean fsoServer) throws IOException {
    // Set the flag to enable/disable FSO on server.
    initOzoneConfiguration(fsoServer);
    createCluster();
  }

  public static void createCluster() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    conf.addResource(CONTRACT_XML);

    BucketLayout bucketLayout = fsOptimizedServer
        ? BucketLayout.FILE_SYSTEM_OPTIMIZED : BucketLayout.LEGACY;
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    try {
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
              180000);
      client = cluster.newClient();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void copyClusterConfigs(String configKey) {
    getConf().set(configKey, cluster.getConf().get(configKey));
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", client);

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    getConf().set("fs.defaultFS", uri);
    copyClusterConfigs(OMConfigKeys.OZONE_OM_ADDRESS_KEY);
    copyClusterConfigs(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);
    copyClusterConfigs(OZONE_FS_HSYNC_ENABLED);
    return FileSystem.get(getConf());
  }

  public static void destroyCluster() throws IOException {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
