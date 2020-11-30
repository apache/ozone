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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
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

  private final SCMRatisServerImpl ratisServer;
  private final ConfigurationSource conf;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManagerImpl(final ConfigurationSource conf) throws IOException {
    this.conf = conf;
    this.ratisServer = new SCMRatisServerImpl(
        conf.getObject(SCMHAConfiguration.class), conf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() throws IOException {
    ratisServer.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLeader() {
    if (!SCMHAUtils.isSCMHAEnabled(conf)) {
      // When SCM HA is not enabled, the current SCM is always the leader.
      return true;
    }
    RaftServer server = ratisServer.getServer();
    Preconditions.checkState(server instanceof RaftServerProxy);
    RaftServerImpl serverImpl = null;
    try {
      // SCM only has one raft group.
      serverImpl = ((RaftServerProxy) server)
          .getImpl(ratisServer.getRaftGroupId());
      if (serverImpl != null) {
        // Only when it's sure the current SCM is the leader, otherwise
        // it should all return false.
        return serverImpl.isLeader();
      }
    } catch (IOException ioe) {
      LOG.error("Fail to get RaftServer impl and therefore it's not clear " +
          "whether it's leader. ", ioe);
    }

    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  private RaftPeerId getPeerIdFromRoleInfo(RaftServerImpl serverImpl) {
    /*
      TODO: Fix Me
              Ratis API has changed.
              RaftServerImpl#getRoleInfoProto is no more public.

    if (serverImpl.isLeader()) {
      return RaftPeerId.getRaftPeerId(
          serverImpl.getRoleInfoProto().getLeaderInfo().toString());
    } else if (serverImpl.isFollower()) {
      return RaftPeerId.valueOf(
          serverImpl.getRoleInfoProto().getFollowerInfo()
              .getLeaderInfo().getId().getId());
    } else {
      return null;
    }
     */
    return null;
  }

  @Override
  public RaftPeer getSuggestedLeader() {
    RaftServer server = ratisServer.getServer();
    Preconditions.checkState(server instanceof RaftServerProxy);
    RaftServerImpl serverImpl = null;
    try {
      // SCM only has one raft group.
      serverImpl = ((RaftServerProxy) server)
          .getImpl(ratisServer.getRaftGroupId());
      if (serverImpl != null) {
        RaftPeerId peerId =  getPeerIdFromRoleInfo(serverImpl);
        if (peerId != null) {
          return RaftPeer.newBuilder().setId(peerId).build();
        }
        return null;
      }
    } catch (IOException ioe) {
      LOG.error("Fail to get RaftServer impl and therefore it's not clear " +
          "whether it's leader. ", ioe);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
  }

  @Override
  public List<String> getRatisRoles() {
    return getRatisServer()
            .getRaftPeers()
            .stream()
            .map(peer -> peer.getAddress() == null ? "" : peer.getAddress())
            .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NotLeaderException triggerNotLeaderException() {
    return new NotLeaderException(RaftGroupMemberId.valueOf(
        ratisServer.getServer().getId(),
        ratisServer.getRaftGroupId()),
        getSuggestedLeader(),
        ratisServer.getRaftPeers());
  }
}
