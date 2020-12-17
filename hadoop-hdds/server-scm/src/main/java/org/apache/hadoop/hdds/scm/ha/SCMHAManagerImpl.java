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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

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
  public Optional<Long> isLeader() {
    if (!SCMHAUtils.isSCMHAEnabled(conf)) {
      // When SCM HA is not enabled, the current SCM is always the leader.
      return Optional.of((long)0);
    }
    RaftServer server = ratisServer.getDivision().getRaftServer();
    Preconditions.checkState(server instanceof RaftServerProxy);
    try {
      // SCM only has one raft group.
      RaftServerImpl serverImpl = ((RaftServerProxy) server)
          .getImpl(ratisServer.getDivision().getGroup().getGroupId());
      if (serverImpl != null) {
        // TODO: getRoleInfoProto() will be exposed from Division later.
        RaftProtos.RoleInfoProto roleInfoProto = serverImpl.getRoleInfoProto();
        return roleInfoProto.hasLeaderInfo()
            ? Optional.of(roleInfoProto.getLeaderInfo().getTerm())
            : Optional.empty();
      }
    } catch (IOException ioe) {
      LOG.error("Fail to get RaftServer impl and therefore it's not clear " +
          "whether it's leader. ", ioe);
    }
    return Optional.empty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
  }
}
