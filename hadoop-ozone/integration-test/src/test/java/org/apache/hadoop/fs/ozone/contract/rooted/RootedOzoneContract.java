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

package org.apache.hadoop.fs.ozone.contract.rooted;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import org.junit.Assert;

/**
 * The contract of Rooted Ozone FileSystem (OFS).
 */
class RootedOzoneContract extends AbstractFSContract {

  private static MiniOzoneCluster cluster;
  private static final String CONTRACT_XML = "contract/ozone.xml";

  RootedOzoneContract(Configuration conf) {
    super(conf);
    // insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return OzoneConsts.OZONE_OFS_URI_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path("/testvol1/testbucket1/test");
  }

  public static void createCluster() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY + "." +
                    "rpc.request.timeout",
            3, TimeUnit.SECONDS);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY + "." +
                    "watch.request.timeout",
            10, TimeUnit.SECONDS);
    conf.addResource(CONTRACT_XML);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    try {
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
              180000);
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
    Assert.assertNotNull("cluster not created", cluster);

    String uri = String.format("%s://localhost:%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME,
        cluster.getOzoneManager().getRpcPort());
    getConf().set("fs.defaultFS", uri);

    // Note: FileSystem#loadFileSystems doesn't load OFS class because
    //  META-INF points to org.apache.hadoop.fs.ozone.OzoneFileSystem
    getConf().set("fs.ofs.impl",
        "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

    copyClusterConfigs(OMConfigKeys.OZONE_OM_ADDRESS_KEY);
    copyClusterConfigs(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);
    return FileSystem.get(getConf());
  }

  public static void destroyCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
