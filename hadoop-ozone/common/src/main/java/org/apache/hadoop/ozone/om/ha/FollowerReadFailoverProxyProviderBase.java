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

package org.apache.hadoop.ozone.om.ha;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the OM follower read failover proxy providers.
 * <p>
 * It keeps the follower read configuration and selects the OM node to send follower reads to.
 * Leader-based failover is delegated to the underlying {@link OMFailoverProxyProviderBase}.
 */
public abstract class FollowerReadFailoverProxyProviderBase {
  private static final Logger LOG = LoggerFactory.getLogger(FollowerReadFailoverProxyProviderBase.class);

  /** Whether follower reads are supported by the OM service. */
  private volatile boolean omServiceSupportsFollowerRead;

  /** Whether eligible reads without an explicit consistency hint use followers. */
  private final boolean defaultFollowerReadEnabled;

  /** The read consistency hint used when follower read is enabled. */
  private final ReadConsistencyHint followerReadConsistency;
  /** The read consistency hint used when follower read is disabled or when follower read fails. */
  private final ReadConsistencyHint leaderReadConsistency;

  /**
   * The index of the OM node used for follower read, in the order of the leader proxy's OM nodes.
   * Should only be accessed in synchronized methods.
   */
  private int currentIndex = 0;

  protected FollowerReadFailoverProxyProviderBase(ReadConsistency followerReadConsistencyType,
      ReadConsistency leaderReadConsistencyType, boolean defaultFollowerReadEnabled) {
    Preconditions.assertTrue(followerReadConsistencyType.allowFollowerRead(),
        "Invalid follower read consistency " + followerReadConsistencyType);
    Preconditions.assertTrue(!leaderReadConsistencyType.allowFollowerRead(),
        "Invalid leader read consistency " + leaderReadConsistencyType);
    // At the start, we don't know whether OM service supports follower read. Therefore, if the client
    // is configured to use follower read, we should assume that OM service supports follower read and
    // only sets this to false if there is an evidence otherwise (i.e. OM throws OMNotLeaderException).
    this.omServiceSupportsFollowerRead = true;
    this.defaultFollowerReadEnabled = defaultFollowerReadEnabled;
    this.followerReadConsistency = followerReadConsistencyType.getHint();
    this.leaderReadConsistency = leaderReadConsistencyType.getHint();
  }

  /** @return the inner proxy provider used for leader-based failover. */
  protected abstract OMFailoverProxyProviderBase<OzoneManagerProtocolPB> getLeaderProxy();

  public boolean isOmServiceSupportsFollowerRead() {
    return omServiceSupportsFollowerRead;
  }

  /**
   * Stop sending reads to OM followers, since an OM follower does not support follower read
   * (i.e. it throws OMNotLeaderException).
   */
  protected void disableFollowerRead() {
    omServiceSupportsFollowerRead = false;
  }

  /**
   * Determines whether an eligible read request may be sent to an OM follower.
   *
   * <p>An explicit consistency hint takes precedence over the configured
   * default follower-read behavior.</p>
   */
  protected boolean shouldUseFollowerRead(OMRequest request) {
    if (!omServiceSupportsFollowerRead || !OmUtils.shouldSendToFollower(request)) {
      return false;
    }
    if (request.hasReadConsistencyHint()) {
      return getReadConsistency(request).allowFollowerRead();
    }
    return defaultFollowerReadEnabled;
  }

  /**
   * Add the default follower or leader read consistency hint to the request
   * if the request does not have a consistency hint.
   */
  protected OMRequest addReadConsistencyHint(OMRequest request, boolean followerRead) {
    if (request.hasReadConsistencyHint()) {
      return request;
    }
    final ReadConsistencyHint hint = followerRead ? followerReadConsistency : leaderReadConsistency;
    return request.toBuilder().setReadConsistencyHint(hint).build();
  }

  protected static ReadConsistency getReadConsistency(OMRequest request) {
    return request.hasReadConsistencyHint()
        ? ReadConsistency.fromProto(request.getReadConsistencyHint().getReadConsistency())
        : ReadConsistency.DEFAULT;
  }

  protected int getOMNodeCount() {
    return getLeaderProxy().getOMProxyMap().size();
  }

  protected synchronized String getCurrentFollowerReadNodeId() {
    return getLeaderProxy().getOMProxyMap().getNodeId(currentIndex);
  }

  /**
   * Move to the next OM node for follower read. If the current node is no longer
   * the given node, the call is ignored; this is to handle concurrent calls
   * (to avoid changing the node multiple times).
   *
   * @param nodeId the expected current node.
   */
  protected synchronized void changeFollowerReadNodeId(String nodeId) {
    if (nodeId.equals(getCurrentFollowerReadNodeId())) {
      currentIndex = (currentIndex + 1) % getOMNodeCount();
      LOG.debug("Changed follower read OM node from {} to {}", nodeId, getCurrentFollowerReadNodeId());
    }
  }

  /**
   * Select the OM node to send a follower read with the given consistency to.
   * A {@link ReadConsistency#LOCAL_LEASE} read skips the leader known by the leader proxy.
   *
   * @return the selected node, or null if no node other than the known leader is available.
   */
  protected synchronized String selectFollowerReadNodeId(ReadConsistency readConsistency) {
    String nodeId = getCurrentFollowerReadNodeId();
    if (readConsistency != ReadConsistency.LOCAL_LEASE) {
      return nodeId;
    }

    final String leaderNodeId = getLeaderProxy().getCurrentProxyOMNodeId();
    for (int i = 0; i < getOMNodeCount(); i++) {
      if (!nodeId.equals(leaderNodeId)) {
        return nodeId;
      }
      changeFollowerReadNodeId(nodeId);
      nodeId = getCurrentFollowerReadNodeId();
    }
    return null;
  }

  public synchronized void changeInitialProxyForTest(String initialOmNodeId) {
    currentIndex = getLeaderProxy().getOMProxyMap().indexOf(initialOmNodeId);
  }
}
