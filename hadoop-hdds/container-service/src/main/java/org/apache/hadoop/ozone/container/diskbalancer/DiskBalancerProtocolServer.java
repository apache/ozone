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

package org.apache.hadoop.ozone.container.diskbalancer;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side implementation of {@link DiskBalancerProtocol} for datanodes.
 */
public class DiskBalancerProtocolServer implements DiskBalancerProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerProtocolServer.class);

  private final DatanodeStateMachine datanodeStateMachine;
  private final PrivilegedOperation adminChecker;

  /**
   * Functional interface for checking admin privileges before executing operations.
   */
  @FunctionalInterface
  public interface PrivilegedOperation {

    /**
     * Check if the current user has admin privileges for the specified operation.
     * @param operation the operation name to check privileges for
     * @throws IOException if the privilege check fails or user is not authorized
     */
    void check(String operation) throws IOException;
  }

  public DiskBalancerProtocolServer(DatanodeStateMachine datanodeStateMachine,
      PrivilegedOperation adminChecker) {
    this.datanodeStateMachine = Objects.requireNonNull(datanodeStateMachine, "datanodeStateMachine");
    this.adminChecker = Objects.requireNonNull(adminChecker, "adminChecker");
  }

  @Override
  public DatanodeDiskBalancerInfoProto getDiskBalancerInfo(GetDiskBalancerInfoRequestProto request) throws IOException {
    // No admin check - both report and status are read-only
    final DiskBalancerInfo info = getDiskBalancerInfoImpl();
    DatanodeDetails datanodeDetails = datanodeStateMachine.getDatanodeDetails();

    return DatanodeDiskBalancerInfoProto.newBuilder()
        .setNode(datanodeDetails.toProto(request.getClientVersion()))
        .setCurrentVolumeDensitySum(info.getVolumeDataDensity())
        .setDiskBalancerConf(DiskBalancerConfigurationProto.newBuilder()
                .setThreshold(info.getThreshold())
                .setDiskBandwidthInMB(info.getBandwidthInMB())
                .setParallelThread(info.getParallelThread())
                .setStopAfterDiskEven(info.isStopAfterDiskEven()))
        .setSuccessMoveCount(info.getSuccessCount())
        .setFailureMoveCount(info.getFailureCount())
        .setBytesToMove(info.getBytesToMove())
        .setBytesMoved(info.getBalancedBytes())
        .setRunningStatus(info.getOperationalState())
        .build();
  }

  @Override
  public void startDiskBalancer(DiskBalancerConfigurationProto configProto)
      throws IOException {
    adminChecker.check("startDiskBalancer");
    final DiskBalancerInfo info = getDiskBalancerInfoImpl();

    // Check node operational state before starting DiskBalancer
    // Only IN_SERVICE nodes should actively balance disks
    NodeOperationalState nodeState = 
        datanodeStateMachine.getDatanodeDetails().getPersistedOpState();
    
    if (nodeState == NodeOperationalState.IN_SERVICE) {
      info.setOperationalState(DiskBalancerRunningStatus.RUNNING);
    } else {
      LOG.warn("Cannot start DiskBalancer as node is in {} state. Pausing instead.", 
          nodeState);
      info.setOperationalState(DiskBalancerRunningStatus.PAUSED);
    }

    DiskBalancerConfiguration finalConfig;
    if (configProto != null) {
      // only update fields present in configProto
      DiskBalancerConfiguration existingConfig = info.toConfiguration();
      finalConfig = DiskBalancerConfiguration.updateFromProtobuf(configProto, existingConfig);
      info.updateFromConf(finalConfig);
    } else {
      finalConfig = info.toConfiguration();
    }

    LOG.info("DiskBalancer opType : START \n{}", finalConfig);
    refreshService(info);
  }

  @Override
  public void stopDiskBalancer() throws IOException {
    adminChecker.check("stopDiskBalancer");
    final DiskBalancerInfo info = getDiskBalancerInfoImpl();
    LOG.info("DiskBalancer opType : STOP");
    info.setOperationalState(DiskBalancerRunningStatus.STOPPED);
    refreshService(info);
  }

  @Override
  public void updateDiskBalancerConfiguration(@Nonnull DiskBalancerConfigurationProto configProto)
      throws IOException {
    adminChecker.check("updateDiskBalancerConfiguration");
    final DiskBalancerInfo info = getDiskBalancerInfoImpl();

    // only update fields present in configProto
    DiskBalancerConfiguration currentConfig = info.toConfiguration();
    DiskBalancerConfiguration updateConfig = DiskBalancerConfiguration.updateFromProtobuf(configProto, currentConfig);
    info.updateFromConf(updateConfig);
    LOG.info("DiskBalancer opType : UPDATE :\n{}", updateConfig);
    refreshService(info);
  }

  private DiskBalancerInfo getDiskBalancerInfoImpl() throws IOException {
    OzoneContainer container = datanodeStateMachine.getContainer();
    if (container.getDiskBalancerService() == null) {
      throw new IOException("DiskBalancer service is disabled on this datanode");
    }
    return container.getDiskBalancerInfo();
  }

  private void refreshService(DiskBalancerInfo info) throws IOException {
    OzoneContainer container = datanodeStateMachine.getContainer();
    DiskBalancerService service = container.getDiskBalancerService();
    if (service == null) {
      throw new IOException("DiskBalancer service is disabled on this datanode");
    }
    service.refresh(info);
  }

  @Override
  public void close() {
    // No resources to clean up
  }
}


