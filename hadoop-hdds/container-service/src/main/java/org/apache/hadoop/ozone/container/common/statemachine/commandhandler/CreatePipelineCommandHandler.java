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
import java.util.List;
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
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for create pipeline command received from SCM.
 */
public class CreatePipelineCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreatePipelineCommandHandler.class);

  private final AtomicLong invocationCount = new AtomicLong(0);
  private final AtomicInteger queuedCount = new AtomicInteger(0);
  private final BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClient;

  private final Executor executor;
  private final MutableRate opsLatencyMs;
  private final Set<UUID> pipelinesInProgress;

  /**
   * Constructs a createPipelineCommand handler.
   */
  public CreatePipelineCommandHandler(ConfigurationSource conf,
                                      Executor executor) {
    this(RatisHelper.newRaftClient(conf), executor);
  }

  CreatePipelineCommandHandler(
      BiFunction<RaftPeer, GrpcTlsConfig, RaftClient> newRaftClient,
      Executor executor) {
    this.newRaftClient = newRaftClient;
    this.executor = executor;
    MetricsRegistry registry = new MetricsRegistry(
        CreatePipelineCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.createPipelineCommand + "Ms");
    this.pipelinesInProgress = ConcurrentHashMap.newKeySet();
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
    final CreatePipelineCommand createCommand = (CreatePipelineCommand) command;
    final PipelineID pipelineID = createCommand.getPipelineID();
    final UUID pipelineUUID = pipelineID.getId();
    
    // Check if this pipeline is already being processed
    if (!pipelinesInProgress.add(pipelineUUID)) {
      LOG.debug("Create Pipeline command for pipeline {} is already in progress, " +
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
        final List<DatanodeDetails> peers = createCommand.getNodeList();
        final List<Integer> priorityList = createCommand.getPriorityList();

        try {
          XceiverServerSpi server = ozoneContainer.getWriteChannel();
          if (!server.isExist(pipelineIdProto)) {
            final RaftGroupId groupId = RaftGroupId.valueOf(pipelineID.getId());
            final RaftGroup group =
                RatisHelper.newRaftGroup(groupId, peers, priorityList);
            server.addGroup(pipelineIdProto, peers, priorityList);
            peers.stream().filter(d -> !d.getID().equals(dn.getID()))
                .forEach(d -> {
                  final RaftPeer peer = RatisHelper.toRaftPeer(d);
                  try (RaftClient client = newRaftClient.apply(peer,
                      ozoneContainer.getTlsClientConfig())) {
                    client.getGroupManagementApi(peer.getId()).add(group);
                  } catch (AlreadyExistsException ae) {
                    // do not log
                  } catch (IOException ioe) {
                    LOG.warn("Add group failed for {}", d, ioe);
                  }
                });
            LOG.info("Created Pipeline {} {} {}.",
                createCommand.getReplicationType(), createCommand.getFactor(),
                pipelineID);
          }
        } catch (IOException e) {
          // The server.addGroup may exec after a getGroupManagementApi call
          // from another peer, so we may got an AlreadyExistsException.
          if (!(e.getCause() instanceof AlreadyExistsException)) {
            LOG.error("Can't create pipeline {} {} {}",
                createCommand.getReplicationType(),
                createCommand.getFactor(), pipelineID, e);
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
      LOG.warn("Create Pipeline command for pipeline {} is rejected as " +
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
    return SCMCommandProto.Type.createPipelineCommand;
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
