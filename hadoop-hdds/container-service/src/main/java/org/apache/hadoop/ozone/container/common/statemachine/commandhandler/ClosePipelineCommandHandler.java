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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for close pipeline command received from SCM.
 */
public class ClosePipelineCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClosePipelineCommandHandler.class);

  private final AtomicLong invocationCount = new AtomicLong(0);
  private final AtomicInteger queuedCount = new AtomicInteger(0);
  private final Executor executor;
  private final BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClient;
  private final MutableRate opsLatencyMs;
  private final Set<UUID> pipelinesInProgress;

  /**
   * Constructs a closePipelineCommand handler.
   */
  public ClosePipelineCommandHandler(ConfigurationSource conf,
                                     Executor executor) {
    this(RatisHelper.newRaftClientNoRetry(conf), executor);
  }

  /**
   * Constructs a closePipelineCommand handler.
   */
  public ClosePipelineCommandHandler(
      BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClient,
      Executor executor) {
    this.newRaftClient = newRaftClient;
    this.executor = executor;
    MetricsRegistry registry = new MetricsRegistry(
        ClosePipelineCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.closePipelineCommand + "Ms");
    this.pipelinesInProgress = ConcurrentHashMap.newKeySet();
  }

  /**
   * Returns true if pipeline close is in progress, else false.
   *
   * @return boolean
   */
  public boolean isPipelineCloseInProgress(UUID pipelineID) {
    return pipelinesInProgress.contains(pipelineID);
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param ozoneContainer    - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand<?> command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    final ClosePipelineCommand closePipelineCommand = (ClosePipelineCommand) command;
    final PipelineID pipelineID = closePipelineCommand.getPipelineID();
    final UUID pipelineUUID = pipelineID.getId();
    
    // Check if this pipeline is already being processed
    if (!pipelinesInProgress.add(pipelineUUID)) {
      LOG.debug("Close Pipeline command for pipeline {} is already in progress, " +
          "skipping duplicate command.", pipelineID);
      return;
    }
    
    try {
      queuedCount.incrementAndGet();
      CompletableFuture.runAsync(() -> {
        invocationCount.incrementAndGet();
        final long startTime = Time.monotonicNow();
        final DatanodeDetails dn = context.getParent().getDatanodeDetails();
        final HddsProtos.PipelineID pipelineIdProto = pipelineID.getProtobuf();

        try {
          XceiverServerSpi server = ozoneContainer.getWriteChannel();
          if (server.isExist(pipelineIdProto)) {
            if (server instanceof XceiverServerRatis) {
              // TODO: Refactor Ratis logic to XceiverServerRatis
              // Propagate the group remove to the other Raft peers in the pipeline
              XceiverServerRatis ratisServer = (XceiverServerRatis) server;
              final RaftGroupId raftGroupId = RaftGroupId.valueOf(pipelineID.getId());
              final boolean shouldDeleteRatisLogDirectory = ratisServer.getShouldDeleteRatisLogDirectory();
              // This might throw GroupMismatchException if the Ratis group has been closed by other datanodes
              final Collection<RaftPeer> peers = ratisServer.getRaftPeersInPipeline(pipelineID);
              // Try to send remove group for the other datanodes first, ignoring GroupMismatchException
              // if the Ratis group has been closed in the other datanodes
              peers.stream()
                  .filter(peer -> !peer.getId().equals(ratisServer.getServer().getId()))
                  .forEach(peer -> {
                    try (RaftClient client = newRaftClient.apply(peer, ozoneContainer.getTlsClientConfig())) {
                      client.getGroupManagementApi(peer.getId())
                          .remove(raftGroupId, shouldDeleteRatisLogDirectory, !shouldDeleteRatisLogDirectory);
                    } catch (GroupMismatchException ae) {
                      // ignore silently since this means that the group has been closed by earlier close pipeline
                      // command in another datanode
                      LOG.debug("Failed to remove group {} for pipeline {} on peer {} since the group has " +
                          "been removed by earlier close pipeline command handled in another datanode", raftGroupId,
                          pipelineID, peer.getId());
                    } catch (IOException ioe) {
                      LOG.warn("Failed to remove group {} of pipeline {} on peer {}",
                          raftGroupId, pipelineID, peer.getId(), ioe);
                    }
                  });
            }
            // Remove the Ratis group from the current datanode pipeline, might throw GroupMismatchException as
            // well. It is a no-op for XceiverServerSpi implementations (e.g. XceiverServerGrpc)
            server.removeGroup(pipelineIdProto);
            LOG.info("Close Pipeline {} command on datanode {}.", pipelineID, dn);
          } else {
            LOG.debug("Ignoring close pipeline command for pipeline {} on datanode {} " +
                "as it does not exist", pipelineID, dn);
          }
        } catch (IOException e) {
          Throwable gme = HddsClientUtils.containsException(e, GroupMismatchException.class);
          if (gme != null) {
            // ignore silently since this means that the group has been closed by earlier close pipeline
            // command in another datanode
            LOG.debug("The group for pipeline {} on datanode {} has been removed by earlier close " +
                "pipeline command handled in another datanode", pipelineID, dn);
          } else {
            LOG.error("Can't close pipeline {}", pipelineID, e);
          }
        } finally {
          long endTime = Time.monotonicNow();
          this.opsLatencyMs.add(endTime - startTime);
        }
      }, executor).whenComplete((v, e) -> {
        queuedCount.decrementAndGet();
        pipelinesInProgress.remove(pipelineUUID);
      });
    } catch (RejectedExecutionException ex) {
      queuedCount.decrementAndGet();
      pipelinesInProgress.remove(pipelineUUID);
      LOG.warn("Close Pipeline command for pipeline {} is rejected as " +
          "command queue has reached max size.", pipelineID);
    }
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.closePipelineCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return (int)invocationCount.get();
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    return (long) this.opsLatencyMs.lastStat().mean();
  }

  @Override
  public long getTotalRunTime() {
    return (long) this.opsLatencyMs.lastStat().total();
  }

  @Override
  public int getQueuedCount() {
    return queuedCount.get();
  }
}
