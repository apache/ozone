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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for close container command received from SCM.
 */
public class CloseContainerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerCommandHandler.class);

  private final AtomicLong invocationCount = new AtomicLong(0);
  private final AtomicInteger queuedCount = new AtomicInteger(0);
  private final ThreadPoolExecutor executor;
  private final MutableRate opsLatencyMs;

  /**
   * Constructs a close container command handler.
   */
  public CloseContainerCommandHandler(
      int threadPoolSize, int queueSize, String threadNamePrefix) {
    executor = new ThreadPoolExecutor(
        threadPoolSize, threadPoolSize,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(queueSize),
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix + "CloseContainerThread-%d")
            .build());
    MetricsRegistry registry = new MetricsRegistry(
        CloseContainerCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.closeContainerCommand + "Ms");
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
    queuedCount.incrementAndGet();
    CompletableFuture.runAsync(() -> {
      invocationCount.incrementAndGet();
      final long startTime = Time.monotonicNow();
      final DatanodeDetails datanodeDetails = context.getParent()
          .getDatanodeDetails();
      final CloseContainerCommandProto closeCommand =
          ((CloseContainerCommand) command).getProto();
      final ContainerController controller = ozoneContainer.getController();
      final long containerId = closeCommand.getContainerID();
      LOG.debug("Processing Close Container command container #{}",
          containerId);
      try {
        final Container container = controller.getContainer(containerId);

        if (container == null) {
          LOG.info("Container #{} does not exist in datanode. "
              + "Its pipeline may have closed before it was created. "
              + "Container close failed.", containerId);
          return;
        }

        // move the container to CLOSING if in OPEN state
        controller.markContainerForClose(containerId);

        switch (container.getContainerState()) {
        case OPEN:
        case CLOSING:
          // If the container is part of open pipeline, close it via
          // write channel
          if (ozoneContainer.getWriteChannel()
              .isExist(closeCommand.getPipelineID())) {
            ContainerCommandRequestProto request =
                getContainerCommandRequestProto(datanodeDetails,
                    closeCommand.getContainerID(),
                    command.getEncodedToken());
            ozoneContainer.getWriteChannel()
                .submitRequest(request, closeCommand.getPipelineID());
          } else if (closeCommand.getForce()) {
            // Non-RATIS containers should have the force close flag set, so
            // they are moved to CLOSED immediately rather than going to
            // quasi-closed
            controller.closeContainer(containerId);
          } else {
            controller.quasiCloseContainer(containerId,
                "Ratis pipeline does not exist");
            LOG.info("Marking Container {} quasi closed", containerId);
          }
          break;
        case QUASI_CLOSED:
          if (closeCommand.getForce()) {
            controller.closeContainer(containerId);
          }
          break;
        case CLOSED:
          break;
        case UNHEALTHY:
        case INVALID:
          LOG.debug("Cannot close the container #{}, the container is"
              + " in {} state.", containerId, container.getContainerState());
          break;
        default:
          break;
        }
      } catch (NotLeaderException e) {
        LOG.info("Follower cannot close container #{}.", containerId);
      } catch (IOException e) {
        LOG.error("Can't close container #{}", containerId, e);
      } finally {
        long endTime = Time.monotonicNow();
        this.opsLatencyMs.add(endTime - startTime);
      }
    }, executor).whenComplete((v, e) -> queuedCount.decrementAndGet());
  }

  private ContainerCommandRequestProto getContainerCommandRequestProto(
      final DatanodeDetails datanodeDetails, final long containerId,
      final String encodedToken) {
    final ContainerCommandRequestProto.Builder command =
        ContainerCommandRequestProto.newBuilder();
    command.setCmdType(ContainerProtos.Type.CloseContainer);
    command.setTraceID(TracingUtil.exportCurrentSpan());
    command.setContainerID(containerId);
    command.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    command.setDatanodeUuid(datanodeDetails.getUuidString());
    if (encodedToken != null) {
      command.setEncodedToken(encodedToken);
    }
    return command.build();
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.closeContainerCommand;
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

  @Override
  public int getThreadPoolMaxPoolSize() {
    return executor.getMaximumPoolSize();
  }

  @Override
  public int getThreadPoolActivePoolSize() {
    return executor.getActiveCount();
  }

  @Override
  public void stop() {
    executor.shutdownNow();
  }
}
