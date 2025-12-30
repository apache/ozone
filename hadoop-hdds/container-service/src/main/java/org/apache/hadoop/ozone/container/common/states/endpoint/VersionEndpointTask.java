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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import java.io.IOException;
import java.net.BindException;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task that returns version.
 */
public class VersionEndpointTask implements
    Callable<EndpointStateMachine.EndPointStates> {
  private static final Logger LOG = LoggerFactory.getLogger(VersionEndpointTask.class);
  private final EndpointStateMachine rpcEndPoint;
  private final ConfigurationSource configuration;
  private final OzoneContainer ozoneContainer;

  public VersionEndpointTask(EndpointStateMachine rpcEndPoint,
      ConfigurationSource conf, OzoneContainer container) {
    this.rpcEndPoint = rpcEndPoint;
    this.configuration = conf;
    this.ozoneContainer = container;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {
    rpcEndPoint.lock();

    try {
      if (rpcEndPoint.getState().equals(
          EndpointStateMachine.EndPointStates.GETVERSION)) {
        SCMVersionResponseProto versionResponse =
            rpcEndPoint.getEndPoint().getVersion(null);
        VersionResponse response = VersionResponse.getFromProtobuf(
            versionResponse);
        rpcEndPoint.setVersion(response);

        if (!rpcEndPoint.isPassive()) {
          // If end point is passive, datanode does not need to check volumes.
          String scmId = response.getValue(OzoneConsts.SCM_ID);
          String clusterId = response.getValue(OzoneConsts.CLUSTER_ID);

          Objects.requireNonNull(scmId, "scmId == null");
          Objects.requireNonNull(clusterId, "clusterId == null");

          // Check DbVolumes, format DbVolume at first register time.
          checkVolumeSet(ozoneContainer.getDbVolumeSet(), scmId, clusterId);

          // Check HddsVolumes
          checkVolumeSet(ozoneContainer.getVolumeSet(), scmId, clusterId);

          // Start the container services after getting the version information
          ozoneContainer.start(clusterId);
        }
        EndpointStateMachine.EndPointStates nextState =
            rpcEndPoint.getState().getNextState();
        rpcEndPoint.setState(nextState);
        rpcEndPoint.zeroMissedCount();
      } else {
        LOG.debug("Cannot execute GetVersion task as endpoint state machine " +
            "is in {} state", rpcEndPoint.getState());
      }
    } catch (DiskOutOfSpaceException | BindException ex) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.SHUTDOWN);
    } catch (IOException ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }
    return rpcEndPoint.getState();
  }

  private void checkVolumeSet(MutableVolumeSet volumeSet,
      String scmId, String clusterId) throws DiskOutOfSpaceException {
    if (volumeSet == null) {
      return;
    }

    volumeSet.writeLock();
    try {
      // If version file does not exist
      // create version file and also set scm ID or cluster ID.
      for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
        boolean result = StorageVolumeUtil.checkVolume(volume,
            scmId, clusterId, configuration, LOG,
            ozoneContainer.getDbVolumeSet());
        if (!result) {
          volumeSet.failVolume(volume.getStorageDir().getPath());
        }
      }
      if (volumeSet.getVolumesList().isEmpty()) {
        // All volumes are in inconsistent state
        throw new DiskOutOfSpaceException(
            "All configured Volumes are in Inconsistent State");
      }
    } finally {
      volumeSet.writeUnlock();
    }
  }
}
