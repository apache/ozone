/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handler to process the DeleteContainerCommand from SCM.
 */
public class DeleteContainerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteContainerCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicLong totalTime = new AtomicLong(0);
  private final ExecutorService executor;

  public DeleteContainerCommandHandler(int threadPoolSize) {
    this.executor = new ThreadPoolExecutor(
        0, threadPoolSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("DeleteContainerThread-%d")
            .build());
  }

  @Override
  public void handle(final SCMCommand command,
                     final OzoneContainer ozoneContainer,
                     final StateContext context,
                     final SCMConnectionManager connectionManager) {
    final DeleteContainerCommand deleteContainerCommand =
        (DeleteContainerCommand) command;
    final ContainerController controller = ozoneContainer.getController();
    executor.execute(() -> {
      final long startTime = Time.monotonicNow();
      invocationCount.incrementAndGet();
      try {
        controller.deleteContainer(deleteContainerCommand.getContainerID(),
            deleteContainerCommand.isForce());
      } catch (IOException e) {
        LOG.error("Exception occurred while deleting the container.", e);
      } finally {
        totalTime.getAndAdd(Time.monotonicNow() - startTime);
      }
    });
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.deleteContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount.get();
  }

  @Override
  public long getAverageRunTime() {
    final int invocations = invocationCount.get();
    return invocations == 0 ?
        0 : totalTime.get() / invocations;
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
