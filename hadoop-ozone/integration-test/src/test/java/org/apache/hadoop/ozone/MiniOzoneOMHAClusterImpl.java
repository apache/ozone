/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.util.List;
import java.util.Collections;

/**
 * MiniOzoneOMHAClusterImpl creates a complete in-process Ozone cluster
 * with OM HA suitable for running tests.  The cluster consists of a set of
 * OzoneManagers, StorageContainerManager and multiple DataNodes.
 */
public final class MiniOzoneOMHAClusterImpl extends MiniOzoneHAClusterImpl {
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  /**
   * Creates a new MiniOzoneOMHACluster.
   *
   * @throws IOException if there is an I/O error
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private MiniOzoneOMHAClusterImpl(
      OzoneConfiguration conf,
      List<OzoneManager> activeOMList,
      List<OzoneManager> inactiveOMList,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceId,
      String metaPath,
      ReconServer reconServer) {
    super(conf, activeOMList, inactiveOMList, Collections.singletonList(scm),
        null, hddsDatanodes, omServiceId, null, metaPath, reconServer);
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneHAClusterImpl.Builder {

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    @Override
    public MiniOzoneCluster build() throws IOException {
      if (numOfActiveOMs > numOfOMs) {
        throw new IllegalArgumentException("Number of active OMs cannot be " +
            "more than the total number of OMs");
      }

      // If num of ActiveOMs is not set, set it to numOfOMs.
      if (numOfActiveOMs == ACTIVE_OMS_NOT_SET) {
        numOfActiveOMs = numOfOMs;
      }

      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      initOMRatisConf();
      StorageContainerManager scm;
      ReconServer reconServer = null;
      try {
        scm = createSCM();
        scm.start();
        createOMService();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          Collections.singletonList(scm), reconServer);

      MiniOzoneClusterImpl cluster = new MiniOzoneOMHAClusterImpl(conf,
          getActiveOMs(), getInactiveOMs(), scm, hddsDatanodes,
          omServiceId, path, reconServer);

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }
  }
}