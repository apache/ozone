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
import java.time.Clock;
import java.util.OptionalLong;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to process the DeleteContainerCommand from SCM.
 */
public class DeleteContainerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteContainerCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicInteger timeoutCount = new AtomicInteger(0);
  private final ThreadPoolExecutor executor;
  private final Clock clock;
  private int maxQueueSize;
  private final MutableRate opsLatencyMs;

  public DeleteContainerCommandHandler(
      int threadPoolSize, Clock clock, int queueSize, String threadNamePrefix) {
    this(clock, new ThreadPoolExecutor(
            threadPoolSize, threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new ThreadFactoryBuilder()
                .setNameFormat(threadNamePrefix + "DeleteContainerThread-%d")
                .build()),
        queueSize);
  }

  protected DeleteContainerCommandHandler(Clock clock,
      ThreadPoolExecutor executor, int queueSize) {
    this.executor = executor;
    this.clock = clock;
    maxQueueSize = queueSize;
    MetricsRegistry registry = new MetricsRegistry(
        DeleteContainerCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(SCMCommandProto.Type.deleteContainerCommand + "Ms");
  }

  @Override
  public void handle(final SCMCommand<?> command,
                     final OzoneContainer ozoneContainer,
                     final StateContext context,
                     final SCMConnectionManager connectionManager) {
    final DeleteContainerCommand deleteContainerCommand =
        (DeleteContainerCommand) command;
    final ContainerController controller = ozoneContainer.getController();
    try {
      executor.execute(() ->
          handleInternal(command, context, deleteContainerCommand, controller));
    } catch (RejectedExecutionException ex) {
      LOG.warn("Delete Container command is received for container {} "
          + "is ignored as command queue reach max size {}.",
          deleteContainerCommand.getContainerID(), maxQueueSize);
    }
  }

  private void handleInternal(SCMCommand<?> command, StateContext context,
      DeleteContainerCommand deleteContainerCommand,
      ContainerController controller) {
    final long startTime = Time.monotonicNow();
    invocationCount.incrementAndGet();
    try {
      if (command.hasExpired(clock.millis())) {
        LOG.info("Not processing the delete container command for " +
            "container {} as the current time {}ms is after the command " +
            "deadline {}ms", deleteContainerCommand.getContainerID(),
            clock.millis(), command.getDeadline());
        timeoutCount.incrementAndGet();
        return;
      }

      if (context != null) {
        final OptionalLong currentTerm = context.getTermOfLeaderSCM();
        final long cmdTerm = command.getTerm();
        if (currentTerm.isPresent() && cmdTerm < currentTerm.getAsLong()) {
          LOG.info("Ignoring delete container command for container {} since " +
              "SCM leader has new term ({} < {})",
              deleteContainerCommand.getContainerID(),
              cmdTerm, currentTerm.getAsLong());
          return;
        }
      }

      controller.deleteContainer(deleteContainerCommand.getContainerID(),
          deleteContainerCommand.isForce());
    } catch (IOException e) {
      LOG.error("Exception occurred while deleting the container.", e);
    } finally {
      this.opsLatencyMs.add(Time.monotonicNow() - startTime);
    }
  }

  @Override
  public int getQueuedCount() {
    return executor.getQueue().size();
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.deleteContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount.get();
  }

  public int getTimeoutCount() {
    return this.timeoutCount.get();
  }

  @Override
  public long getAverageRunTime() {
    return (long) this.opsLatencyMs.lastStat().mean();
  }

  @Override
  public long getTotalRunTime() {
    return (long) this.opsLatencyMs.lastStat().total();
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
    try {
      executor.shutdown();
      if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      // Ignore, we don't really care about the failure.
      Thread.currentThread().interrupt();
    }
  }

}
