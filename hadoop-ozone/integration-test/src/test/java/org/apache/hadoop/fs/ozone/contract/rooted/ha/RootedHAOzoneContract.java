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

package org.apache.hadoop.fs.ozone.contract.rooted.ha;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import org.junit.Assert;

/**
 * The contract of Rooted Ozone FileSystem (OFS).
 */
class RootedHAOzoneContract extends AbstractFSContract {

  private static MiniOzoneHAClusterImpl cluster;
  private static final String OM_SERVICE_ID = "omservice";
  private static final String CONTRACT_XML = "contract/ozone.xml";

  RootedHAOzoneContract(Configuration conf) {
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
    // Init HA cluster
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_SERVER_PREFIX_KEY + "." +
                    DatanodeRatisServerConfig.RATIS_SERVER_REQUEST_TIMEOUT_KEY,
            3, TimeUnit.SECONDS);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_SERVER_PREFIX_KEY + "." +
                    DatanodeRatisServerConfig.
                            RATIS_SERVER_WATCH_REQUEST_TIMEOUT_KEY,
            10, TimeUnit.SECONDS);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY + "." +
                    "rpc.request.timeout",
            3, TimeUnit.SECONDS);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY + "." +
                    "watch.request.timeout",
            10, TimeUnit.SECONDS);
    // Ref: TestReconWithOzoneManagerHA#setup
    conf.set(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, Boolean.TRUE.toString());
    // Sync to disk enabled
    conf.set("hadoop.hdds.db.rocksdb.writeoption.sync",
        Boolean.TRUE.toString());
    conf.addResource(CONTRACT_XML);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(OM_SERVICE_ID)
        .setNumDatanodes(3)
        .setNumOfOzoneManagers(3)
        .includeRecon(false)
        .build();
    try {
      cluster.waitForClusterToBeReady();
      // TODO: Wait for leader to be elected?
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

    String uri = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, OM_SERVICE_ID);
    getConf().set("fs.defaultFS", uri);

    // Note: FileSystem#loadFileSystems doesn't load OFS class because
    //  META-INF points to org.apache.hadoop.fs.ozone.OzoneFileSystem
    getConf().set("fs.ofs.impl",
        "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

    // Copy HA configs
    copyClusterConfigs(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);
    copyClusterConfigs(OMConfigKeys.OZONE_OM_NODES_KEY + "." + OM_SERVICE_ID);
    // TODO: Decode OMConfigKeys.OZONE_OM_NODES_KEY + "." + omServiceId
    //  instead of hardcoding
    copyClusterConfigs(OMConfigKeys.OZONE_OM_ADDRESS_KEY + "." +
        OM_SERVICE_ID + ".omNode-1");
    copyClusterConfigs(OMConfigKeys.OZONE_OM_ADDRESS_KEY + "." +
        OM_SERVICE_ID + ".omNode-2");
    copyClusterConfigs(OMConfigKeys.OZONE_OM_ADDRESS_KEY + "." +
        OM_SERVICE_ID + ".omNode-3");
    // TODO: REMOVE
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
