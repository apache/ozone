/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * SCMHAManagerImpl uses Apache Ratis for HA implementation. We will have 2N+1
 * node Ratis ring. The Ratis ring will have one Leader node and 2N follower
 * nodes.
 *
 * TODO
 *
 */
public class SCMHAManagerImpl implements SCMHAManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMHAManagerImpl.class);

  private final SCMRatisServer ratisServer;
  private final ConfigurationSource conf;
  private final SCMDBTransactionBuffer transactionBuffer;
  private final SCMSnapshotProvider scmSnapshotProvider;
  private final StorageContainerManager scm;

  // this should ideally be started only in a ratis leader
  private final InterSCMGrpcProtocolService grpcServer;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManagerImpl(final ConfigurationSource conf,
      final StorageContainerManager scm) throws IOException {
    this.conf = conf;
    this.scm = scm;
    this.transactionBuffer =
        new SCMDBTransactionBuffer(scm);
    this.ratisServer = new SCMRatisServerImpl(conf, scm, transactionBuffer);
    this.scmSnapshotProvider = new SCMSnapshotProvider(conf,
        scm.getSCMHANodeDetails().getPeerNodeDetails());
    grpcServer = new InterSCMGrpcProtocolService(conf, scm);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() throws IOException {
    ratisServer.start();
    if (ratisServer.getDivision().getGroup().getPeers().isEmpty()) {
      // this is a bootstrapped node
      // It will first try to add itself to existing ring
      boolean success = HAUtils.addSCM(OzoneConfiguration.of(conf),
          new AddSCMRequest.Builder().setClusterId(scm.getClusterId())
              .setScmId(scm.getScmId()).setRatisAddr(
              scm.getSCMHANodeDetails().getLocalNodeDetails()
                  // TODO : Should we use IP instead of hostname??
                  .getRatisHostPortStr()).build(), scm.getSCMNodeId());
      if (!success) {
        throw new IOException("Adding SCM to existing HA group failed");
      }
    } else {
      LOG.info(" scm role is {} peers {}",
          ratisServer.getDivision().getInfo().getCurrentRole(),
          ratisServer.getDivision().getGroup().getPeers());
    }
    grpcServer.start();
  }

  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  @Override
  public DBTransactionBuffer getDBTransactionBuffer() {
    return transactionBuffer;
  }

  @Override
  public SCMSnapshotProvider getSCMSnapshotProvider() {
    return scmSnapshotProvider;
  }

  /**
   * Download and install latest checkpoint from leader OM.
   *
   * @param leaderId peerNodeID of the leader OM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  public TermIndex installSnapshotFromLeader(String leaderId) {
    if(scmSnapshotProvider == null) {
      LOG.error("OM Snapshot Provider is not configured as there are no peer " +
          "nodes.");
      return null;
    }

    DBCheckpoint omDBCheckpoint = getDBCheckpointFromLeader(leaderId);
    LOG.info("Downloaded checkpoint from Leader {} to the location {}",
        leaderId, omDBCheckpoint.getCheckpointLocation());

    TermIndex termIndex = null;
    try {
      termIndex = installCheckpoint(leaderId, omDBCheckpoint);
    } catch (Exception ex) {
      LOG.error("Failed to install snapshot from Leader OM.", ex);
    }
    return termIndex;
  }

  /**
   * Install checkpoint. If the checkpoints snapshot index is greater than
   * SCM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied.
   */
  TermIndex installCheckpoint(String leaderId, DBCheckpoint omDBCheckpoint)
      throws Exception {
    // TODO : implement install checkpoint
    return null;
  }


  /**
   * Download the latest SCM DB checkpoint from the leader OM.
   *
   * @param leaderId OMNodeID of the leader OM node.
   * @return latest DB checkpoint from leader OM.
   */
  private DBCheckpoint getDBCheckpointFromLeader(String leaderId) {
    LOG.info("Downloading checkpoint from leader SCM {} and reloading state " +
        "from the checkpoint.", leaderId);

    try {
      return scmSnapshotProvider.getSCMDBSnapshot(leaderId);
    } catch (IOException e) {
      LOG.error("Failed to download checkpoint from OM leader {}", leaderId, e);
    }
    return null;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
    grpcServer.stop();
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    String clusterId = scm.getClusterId();
    if (!request.getClusterId().equals(scm.getClusterId())) {
      throw new IOException(
          "SCM " + request.getScmId() + " with addr " + request.getRatisAddr()
              + " has cluster Id " + request.getClusterId()
              + " but leader SCM cluster id is " + clusterId);
    }
    Preconditions.checkNotNull(
        getRatisServer().getDivision().getGroup().getGroupId());
    return getRatisServer().addSCM(request);
  }
}
