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

package org.apache.hadoop.ozone.jobworker.commands;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerCommandProcessor handles the command processing thread lifecycle
 * and coordination with the JobworkerCommandManager and JobworkerCommandDispatcher.
 */
public class JobworkerCommandProcessor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(JobworkerCommandProcessor.class);

  private final JobworkerStateContext context;
  private final JobworkerCommandManager commandManager;
  private final JobworkerCommandDispatcher commandDispatcher;
  private final ConfigurationSource conf;
  private final String threadNamePrefix;
  private final AtomicLong nextHB;
  private final AtomicLong commandsHandled = new AtomicLong(0);

  private volatile Thread cmdProcessThread = null;
  private volatile boolean running = false;

  /**
   * Creates a new command processor.
   *
   * @param context           context to use
   * @param commandManager    command manager to use
   * @param commandDispatcher command dispatcher to use
   * @param conf              configuration
   * @param threadNamePrefix  prefix for thread names
   * @param nextHB            reference to next heartbeat time
   */
  public JobworkerCommandProcessor(
      JobworkerStateContext context,
      JobworkerCommandManager commandManager,
      JobworkerCommandDispatcher commandDispatcher,
      ConfigurationSource conf,
      String threadNamePrefix,
      AtomicLong nextHB) {
    this.context = context;
    this.commandManager = commandManager;
    this.commandDispatcher = commandDispatcher;
    this.conf = conf;
    this.threadNamePrefix = threadNamePrefix;
    this.nextHB = nextHB;
  }

  /**
   * Start the command processor thread.
   */
  public void start() {
    running = true;
    cmdProcessThread = getCommandHandlerThread(this::processCommandQueue);
    cmdProcessThread.start();
    LOG.info("Started command processor thread");
  }

  /**
   * Stop the command processor thread.
   */
  public void stop() {
    running = false;
    if (cmdProcessThread != null) {
      cmdProcessThread.interrupt();
      cmdProcessThread = null;
    }
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  /**
   * Returns the command processing thread.
   *
   * @return The command processing thread
   */
  public Thread getCommandProcessThread() {
    return cmdProcessThread;
  }

  /**
   * Returns the number of commands handled.
   *
   * @return Command count
   */
  public long getCommandsHandled() {
    return commandsHandled.get();
  }

  /**
   * Task that periodically checks if we have any outstanding commands.
   * It processes commands one by one until the queue is empty, then waits
   * until the next heartbeat + 1 second.
   */
  private void processCommandQueue() {
    long now;
    while (running && context.getState() != JobworkerStates.SHUTDOWN) {
      JobworkerCommand command = commandManager.getNextCommand();
      if (command != null) {
        boolean handled = commandDispatcher.handle(command);
        commandsHandled.incrementAndGet();
        if (!handled) {
          commandManager.updateCommand(command, CommandStatus.Status.FAILED,
              "Command cannot be handled", CommandResultCode.INVALID_COMMAND);
        }
      } else {
        try {
          // Sleep till the next HB + 1 second.
          now = Time.monotonicNow();
          if (nextHB.get() > now) {
            Thread.sleep((nextHB.get() - now) + 1000L);
          }
        } catch (InterruptedException e) {
          // Ignore this exception.
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Create a command handler thread.
   *
   * @param processCommandQueue the runnable to process commands
   * @return the thread
   */
  private Thread getCommandHandlerThread(Runnable processCommandQueue) {
    Thread handlerThread = new Thread(processCommandQueue);
    handlerThread.setDaemon(true);
    handlerThread.setName(threadNamePrefix + "CommandProcessorThread");
    handlerThread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
      // Let us just restart this thread after logging a critical error.
      // if this thread is not running we cannot handle commands from OM.
      LOG.error("Critical Error : Command processor thread encountered an " +
          "error. Thread: {}", t.toString(), e);
      if (running) {
        try {
          long delayMs = 2000L;
          LOG.warn("Restarting the command processor thread after {} ms delay", delayMs);
          Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOG.warn("Thread restart delay interrupted", ie);
        }
        start();
      }
    });
    return handlerThread;
  }
}
