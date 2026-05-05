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

package org.apache.ozone.test;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for Ozone integration tests.  Manages lifecycle of {@link MiniOzoneCluster}.
 * <p/>
 * Subclasses can tweak configuration by overriding {@link #createOzoneConfig()}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ClusterForTests<C extends MiniOzoneCluster> {

  private C cluster;

  /**
   * Creates the base configuration for tests.  This can be tweaked
   * in subclasses by overriding {@link #createOzoneConfig()}.
   */
  protected static OzoneConfiguration createBaseConfiguration() {
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

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setBoolean(OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
    conf.setTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT, 0, TimeUnit.SECONDS);

    return conf;
  }

  /**
   * Hook method that allows tweaking the configuration.
   */
  protected OzoneConfiguration createOzoneConfig() {
    return createBaseConfiguration();
  }

  /**
   * Hook method to create cluster with different parameters.
   */
  protected abstract C createCluster() throws Exception;

  protected C getCluster() {
    return cluster;
  }

  /** Hook method for subclasses. */
  protected MiniOzoneCluster.Builder newClusterBuilder() {
    return MiniOzoneCluster.newBuilder(createOzoneConfig())
        .setNumDatanodes(5);
  }

  /** Hook method for subclasses. */
  protected void onClusterReady() throws Exception {
    // override if needed
  }

  @BeforeAll
  void startCluster() throws Exception {
    cluster = createCluster();
    cluster.waitForClusterToBeReady();
    onClusterReady();
  }

  @AfterAll
  void shutdownCluster() {
    IOUtils.closeQuietly(cluster);
  }

}
