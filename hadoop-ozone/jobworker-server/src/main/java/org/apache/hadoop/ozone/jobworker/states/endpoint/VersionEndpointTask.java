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

package org.apache.hadoop.ozone.jobworker.states.endpoint;

import static org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates.SHUTDOWN;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task that gets the version information from OM.
 */
public class VersionEndpointTask implements Callable<EndpointStates> {
  public static final Logger LOG = LoggerFactory.getLogger(VersionEndpointTask.class);
  private static volatile String verifiedClusterId = null;
  private final JobworkerEndpointStateMachine rpcEndPoint;
  private static final Map<String, String> OM_SERVICE_ID_MAPPING = new ConcurrentHashMap<>();
  private final JobworkerVolumeSet jobworkerVolumeSet;

  /**
   * Constructs VersionEndpointTask.
   *
   * @param rpcEndPoint        - RPC endPoint.
   * @param jobworkerVolumeSet - jobworkerVolumeSet
   */
  public VersionEndpointTask(JobworkerEndpointStateMachine rpcEndPoint,
                             JobworkerVolumeSet jobworkerVolumeSet) {
    this.rpcEndPoint = rpcEndPoint;
    this.jobworkerVolumeSet = jobworkerVolumeSet;

  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStates call() throws Exception {
    rpcEndPoint.lock();
    try {
      if (rpcEndPoint.getState().equals(EndpointStates.GETVERSION)) {
        GetOMVersionRequest versionRequest = GetOMVersionRequest.newBuilder().build();
        GetOMVersionResponse versionResponse =
            rpcEndPoint.getEndPoint().getOMVersion(versionRequest);
        if (versionResponse.getDurableJobProtocolVersion() != 1) {
          LOG.warn("OM {} has no finalized durable Jobworker protocol; waiting for upgrade/finalization", rpcEndPoint);
          return rpcEndPoint.getState();
        }
        Map<String, String> keys = versionResponse.getKeysList().stream()
            .collect(Collectors.toMap(HddsProtos.KeyValue::getKey,
                HddsProtos.KeyValue::getValue));

        String omId = keys.get(OzoneConsts.OM_ID);
        String clusterId = keys.get(OzoneConsts.CLUSTER_ID);
        String serviceId = keys.get(OzoneConsts.OM_SERVICE_ID);
        Objects.requireNonNull(omId,
            "Reply from OM: omId cannot be null");
        Objects.requireNonNull(clusterId,
            "Reply from OM: clusterId cannot be null");
        Objects.requireNonNull(serviceId,
            "Reply from OM: serviceId cannot be null");
        // A jobworker can only serve one cluster
        synchronized (VersionEndpointTask.class) {
          verifiedClusterId(clusterId, serviceId, omId);
          validateAndSetOMServiceIdFromOM(serviceId);
        }
        jobworkerVolumeSet.initializeVolumeSet(clusterId);
        rpcEndPoint.setVersion(versionResponse);
        // Move to the next state - REGISTER
        EndpointStates nextState = rpcEndPoint.getState().getNextState();
        rpcEndPoint.setState(nextState);
        rpcEndPoint.zeroMissedCount();
        LOG.info("Successfully get OzoneManager Version from {}, clusterId {}, serviceId {}, omId {}",
            rpcEndPoint, clusterId, serviceId, omId);
      } else {
        LOG.debug("Cannot execute GetVersion task as endpoint state machine " +
            "is in {} state", rpcEndPoint.getState());
      }
    } catch (Exception ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }
    return rpcEndPoint.getState();
  }

  private void verifiedClusterId(String clusterId, String serviceId, String omId) {
    if (verifiedClusterId == null) {
      // First OM to respond
      verifiedClusterId = clusterId;
      LOG.info("First OM reported cluster ID: {} {}({})", clusterId, serviceId, omId);
    } else if (!verifiedClusterId.equals(clusterId)) {
      String errorMsg = String.format("A jobworker can only serve one cluster, " +
          "cluster ID mismatch between OMs. %s != %s ", verifiedClusterId, clusterId);
      LOG.error(errorMsg);
      rpcEndPoint.setState(SHUTDOWN);
      throw new IllegalStateException(errorMsg);
    }
  }

  /**
   * Validates and sets the actual OMServiceId reported by the OM,
   * mapping it to the originally configured OMServiceId on the client side.
   *
   * <p>This is used to ensure a one-to-one mapping between the client's configured
   * OMServiceId and the OM-reported serviceId. If one configured serviceId maps to
   * multiple actual OM-reported serviceIds, this likely indicates inconsistent
   * configurations in the cluster and will trigger an error.</p>
   *
   * @param reportedOMServiceId the OMServiceId as reported by the OM
   */
  private void validateAndSetOMServiceIdFromOM(String reportedOMServiceId) {
    String configuredOMServiceId = rpcEndPoint.getConfiguredOmServiceId();
    String existingMappedServiceId = OM_SERVICE_ID_MAPPING.putIfAbsent(configuredOMServiceId, reportedOMServiceId);
    if (existingMappedServiceId != null && !existingMappedServiceId.equals(reportedOMServiceId)) {
      String errorMsg = String.format(
          "Conflicting OMServiceId mapping detected: the configured OMServiceId '%s' is already mapped to '%s', "
              + "but the OM reported a different OMServiceId '%s' "
              + "rpcEndPoint=%s",
          configuredOMServiceId, existingMappedServiceId, reportedOMServiceId, rpcEndPoint);
      LOG.error(errorMsg);
      rpcEndPoint.setState(EndpointStates.SHUTDOWN);
      throw new IllegalStateException(errorMsg);
    }
    rpcEndPoint.setOmServiceId(reportedOMServiceId);
  }

  @VisibleForTesting
  public static void resetClusterId() {
    verifiedClusterId = null;
  }

}
