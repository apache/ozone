package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReconcileContainerCommandHandler implements CommandHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerCommandHandler.class);

  private final AtomicLong invocationCount;
  private final AtomicInteger queuedCount;
  private final ExecutorService executor;
  private long totalTime;

  public ReconcileContainerCommandHandler(String threadNamePrefix) {
    invocationCount = new AtomicLong(0);
    queuedCount = new AtomicInteger(0);
    // TODO Allow configurable thread pool size with a default value when the implementation is ready.
    executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "ReconcileContainerThread-%d")
        .build());
    totalTime = 0;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container, StateContext context, SCMConnectionManager connectionManager) {
    queuedCount.incrementAndGet();
    CompletableFuture.runAsync(() -> {
      queuedCount.incrementAndGet();
      long startTime = Time.monotonicNow();
      ReconcileContainerCommand reconcileCommand = (ReconcileContainerCommand) command;
      try {
        container.getController().reconcileContainer(reconcileCommand.getContainerID(),
            reconcileCommand.getSourceDatanodes());
      } catch (IOException ex) {
        LOG.error("Failed to reconcile container {}.", reconcileCommand.getContainerID(), ex);
      } finally {
        long endTime = Time.monotonicNow();
        totalTime += endTime - startTime;
      }
    }, executor).whenComplete((v, e) -> queuedCount.decrementAndGet());
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.reconcileContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return 0;
  }

  @Override
  public long getAverageRunTime() {
    return 0;
  }

  @Override
  public long getTotalRunTime() {
    return 0;
  }

  @Override
  public int getQueuedCount() {
    return 0;
  }
}
