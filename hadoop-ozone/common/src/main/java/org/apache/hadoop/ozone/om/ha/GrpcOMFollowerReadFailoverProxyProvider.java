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

import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getLeaderNotReadyException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getNotLeaderException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getReadException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getReadIndexException;

import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Follower read support for {@link GrpcOmTransport}.
 * <p>
 * If follower read is enabled, a read request is sent to the current follower read OM node,
 * which can be either a leader or a follower. If the OM node fails, the request is sent
 * to the next OM node. If all the OM nodes have failed, the request falls back to the leader OM.
 * <p>
 * Write requests, and read requests when follower read is disabled, are sent to the leader OM.
 */
public class GrpcOMFollowerReadFailoverProxyProvider extends FollowerReadFailoverProxyProviderBase {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcOMFollowerReadFailoverProxyProvider.class);

  /** The inner proxy provider used for leader-based failover. */
  private final GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy;

  public GrpcOMFollowerReadFailoverProxyProvider(GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy,
      ReadConsistency followerReadConsistencyType, ReadConsistency leaderReadConsistencyType,
      boolean defaultFollowerReadEnabled) {
    super(followerReadConsistencyType, leaderReadConsistencyType, defaultFollowerReadEnabled);
    this.leaderProxy = leaderProxy;
  }

  @Override
  protected GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB> getLeaderProxy() {
    return leaderProxy;
  }

  /**
   * Submit the request to an OM follower if follower read is used, otherwise to the leader OM.
   *
   * @param payload the request to submit.
   * @param submitToHost submits a request to the OM at the given gRPC address.
   * @param submitToLeader submits a request to the leader OM, failing over between the OMs if needed.
   * @return the response of the request.
   */
  public OMResponse submitRequest(OMRequest payload,
      CheckedBiFunction<OMRequest, String, OMResponse, IOException> submitToHost,
      CheckedFunction<OMRequest, OMResponse, IOException> submitToLeader) throws IOException {
    if (shouldUseFollowerRead(payload)) {
      OMRequest followerPayload = addReadConsistencyHint(payload, true);
      ReadConsistency readConsistency = getReadConsistency(payload);
      int failedCount = 0;
      for (int i = 0; i < getOMNodeCount(); i++) {
        String nodeId = selectFollowerReadNodeId(readConsistency);
        if (nodeId == null) {
          break;
        }
        String followerHost = leaderProxy.getGrpcProxyAddress(nodeId);
        try {
          OMResponse response = submitToHost.apply(followerPayload, followerHost);
          LOG.debug("Invocation with cmdType {} using follower read host {} was successful",
              followerPayload.getCmdType(), followerHost);
          return response;
        } catch (StatusRuntimeException e) {
          LOG.debug("Invocation with cmdType {} using follower read host {} failed",
              followerPayload.getCmdType(), followerHost, e);
          Exception unwrapped = GrpcOmTransport.unwrapException(new Exception(e));
          if (getNotLeaderException(unwrapped) != null) {
            LOG.debug("Encountered OMNotLeaderException from {}. "
                + "Disable OM follower read and retry OM leader directly.", followerHost);
            disableFollowerRead();
            break;
          }
          if (getLeaderNotReadyException(unwrapped) != null) {
            break;
          }
          ReadIndexException readIndexException = getReadIndexException(unwrapped);
          ReadException readException = getReadException(unwrapped);
          if (readIndexException != null || readException != null ||
              leaderProxy.shouldFailover(unwrapped)) {
            failedCount++;
            changeFollowerReadNodeId(nodeId);
          } else {
            throw e;
          }
        }
      }
      if (failedCount > 0) {
        LOG.warn("{} nodes have failed for read request with cmdType {}. Falling back to leader.",
            failedCount, payload.getCmdType());
      }
    }

    // Either follower read is not used, or no OM node has served the request.
    return submitToLeader.apply(addReadConsistencyHint(payload, false));
  }
}
