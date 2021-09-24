/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ScmInfo wraps the result returned from SCM#getScmInfo which
 * contains clusterId and the SCM Id.
 */
public final class ScmInfo {
  private final String clusterId;
  private final String scmId;
  private final List<String> peerRoles;
  private final RaftGroupId raftGroupId;
  private final Collection<RaftPeer> peers;

  /**
   * Builder for ScmInfo.
   */
  public static class Builder {
    private String clusterId;
    private String scmId;
    private List<String> peerRoles;
    private RaftGroupId raftGroupId;
    private Collection<RaftPeer> peers;

    public Builder() {
      peerRoles = new ArrayList<>();
    }

    /**
     * sets the cluster id.
     * @param cid clusterId to be set
     * @return Builder for ScmInfo
     */
    public Builder setClusterId(String cid) {
      this.clusterId = cid;
      return this;
    }

    /**
     * sets the scmId.
     * @param id scmId
     * @return Builder for scmInfo
     */
    public Builder setScmId(String id) {
      this.scmId = id;
      return this;
    }

    /**
     * Set peer address in Scm HA.
     * @param roles ratis peer address in the format of [ip|hostname]:port
     * @return  Builder for scmInfo
     */
    public Builder setRatisPeerRoles(List<String> roles) {
      peerRoles.addAll(roles);
      return this;
    }

    public Builder setRaftGroupId(RaftGroupId gid) {
      this.raftGroupId = gid;
      return this;
    }

    public Builder setPeers(Collection<RaftPeer> raftPeers) {
      this.peers = raftPeers;
      return this;
    }

    public ScmInfo build() {
      return new ScmInfo(clusterId, scmId, peerRoles, raftGroupId, peers);
    }
  }

  private ScmInfo(String clusterId, String scmId, List<String> peerRoles,
                  RaftGroupId raftGroupId, Collection<RaftPeer> peers) {
    this.clusterId = clusterId;
    this.scmId = scmId;
    this.peerRoles = peerRoles;
    this.raftGroupId = raftGroupId;
    this.peers = peers;
  }

  /**
   * Gets the clusterId from the Version file.
   * @return ClusterId
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Gets the SCM Id from the Version file.
   * @return SCM Id
   */
  public String getScmId() {
    return scmId;
  }

  /**
   * Gets the list of peer roles (currently address) in Scm HA.
   * @return List of peer address
   */
  public List<String> getRatisPeerRoles() {
    return peerRoles;
  }

  /**
   * Gets raft group id.
   * @return the raft group id
   */
  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }

  /**
   * Gets Raft peers.
   * @return the peers
   */
  public Collection<RaftPeer> getPeers() {
    return peers;
  }
}
