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

package org.apache.hadoop.ozone.jobworker;

import static org.apache.hadoop.hdds.server.ServerUtils.executorServiceShutdownGraceful;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.commands.AbstractJobworkerCommandHandler;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandDispatcher;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandHandler;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandJournal;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandManager;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandProcessor;
import org.apache.hadoop.ozone.jobworker.report.JobworkerReportManager;
import org.apache.hadoop.ozone.jobworker.states.InitJobworkerState;
import org.apache.hadoop.ozone.jobworker.states.JobworkerStateHandler;
import org.apache.hadoop.ozone.jobworker.states.RunningJobworkerState;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.ozone.jobworker.volume.VolatileJobworkerVolumeSet;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerStateMachine manages the state machine and lifecycle of JobWorker.
 */

public class JobworkerStateMachine implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerStateMachine.class);

  private final ExecutorService executorService;
  private final ConfigurationSource conf;
  private final JobworkerConnectionManager connectionManager;
  private final JobworkerVolumeSet volumeSet;
  private final AtomicLong nextHB;
  private final JobworkerCommandDispatcher commandDispatcher;
  private final JobworkerCommandManager commandManager;
  private final JobworkerCommandProcessor commandProcessor;
  private final JobworkerStopService jobworkerStopService;
  private JobworkerStateContext context;
  private volatile Thread stateMachineThread = null;
  private final JobworkerReportManager reportManager;

  /**
   * Constructs a jobworker state machine.
   *
   * @param jobworkerDetails     Details of this jobworker
   * @param conf                 Configuration
   * @param jobworkerStopService Service to stop jobworker
   */
  public JobworkerStateMachine(JobworkerDetails jobworkerDetails,
                               ConfigurationSource conf,
                               JobworkerStopService jobworkerStopService
  ) throws IOException {
    this(jobworkerDetails, conf, jobworkerStopService, null);
  }

  public JobworkerStateMachine(JobworkerDetails jobworkerDetails, ConfigurationSource conf,
      JobworkerStopService jobworkerStopService,
      JobworkerCommandJournal journal,
      JobworkerCommandHandler... handlers) throws IOException {
    this.jobworkerStopService = jobworkerStopService;
    this.conf = conf;

    String threadNamePrefix = "JobWorker-" + jobworkerDetails.getUuidString() + "-";
    this.executorService = Executors.newFixedThreadPool(
        1, // TODO Jobworker From configuration
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix + "TaskThread-%d")
            .build());
    this.volumeSet = new VolatileJobworkerVolumeSet(jobworkerDetails.getUuidString(), conf, context);
    this.connectionManager = new JobworkerConnectionManager(conf);
    this.commandManager = new JobworkerCommandManager(conf);
    if (journal != null) {
      this.commandManager.setJournal(journal);
    }
    this.context = new JobworkerStateContext(
        this.conf, JobworkerStates.getInitState(), jobworkerDetails, threadNamePrefix, this, commandManager);
    this.nextHB = new AtomicLong(Time.monotonicNow());
    this.reportManager = JobworkerReportManager.newBuilder(conf)
        .setStateContext(context)
        .addThreadNamePrefix(context.getThreadNamePrefix())
        .addPublisherFor(JobworkerServiceProtocolProtos.JobworkerNodeReportProto.class)
        .addPublisherFor(JobworkerServiceProtocolProtos.CommandStatusReportsProto.class)
        .build();
    reportManager.init();

    JobworkerCommandDispatcher.Builder dispatcherBuilder = JobworkerCommandDispatcher.newBuilder()
        // Add more handlers as they're implemented
        .setConnectionManager(connectionManager)
        .setContext(context);
    for (JobworkerCommandHandler handler : handlers) {
      dispatcherBuilder.addHandler(handler);
    }
    this.commandDispatcher = dispatcherBuilder.build();
    this.commandProcessor = new JobworkerCommandProcessor(
        context, commandManager, commandDispatcher, conf, threadNamePrefix, nextHB);
  }

  /**
   * Returns the Connection manager for this state machine.
   *
   * @return JobworkerConnectionManager
   */
  public JobworkerConnectionManager getConnectionManager() {
    return connectionManager;
  }

  /**
   * Returns the volume set for this JobWorker.
   *
   * @return volume set
   */
  public JobworkerVolumeSet getVolumeSet() {
    return volumeSet;
  }

  /**
   * Gets the current context.
   *
   * @return JobworkerStateContext
   */
  public JobworkerStateContext getContext() {
    return context;
  }

  /**
   * Sets the current context.
   *
   * @param context - Context
   */
  public void setContext(JobworkerStateContext context) {
    this.context = context;
  }

  @VisibleForTesting
  public void setHandlerInjector(OMJobworkerCommandProto.Type commandType,
      FaultInjector injector) {
    if (commandDispatcher.getHandler(commandType) instanceof AbstractJobworkerCommandHandler) {
      ((AbstractJobworkerCommandHandler) commandDispatcher.getHandler(commandType))
          .setInjector(injector);
    }
  }

  /**
   * Get the task for the current state.
   *
   * @return task to execute
   */
  @SuppressWarnings("unchecked")
  public JobworkerStateHandler<JobworkerStates> getTask() {
    switch (context.getState()) {
    case INIT:
      return new InitJobworkerState(this.conf, getConnectionManager(), context);
    case RUNNING:
      return new RunningJobworkerState(this.conf, getConnectionManager(), context, volumeSet);
    case SHUTDOWN:
      return null;
    default:
      throw new IllegalArgumentException("Not Implemented yet.");
    }
  }

  /**
   * Runs the state machine at a fixed frequency.
   */
  private void startStateMachineThread() throws IOException {
    // Start the command processor
    commandProcessor.start();

    long now;
    // TODO jobworker implement ReportManager

    while (context.getState() != JobworkerStates.SHUTDOWN) {
      try {
        LOG.debug("Executing cycle Number : {}", context.getExecutionCount());
        long heartbeatFrequency = context.getHeartbeatFrequencyMs();
        nextHB.set(Time.monotonicNow() + heartbeatFrequency);
        context.execute(executorService, heartbeatFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // Someone has sent interrupt signal, this could be because
        // 1. Trigger heartbeat immediately
        // 2. Shutdown has be initiated.
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Unable to finish the execution.", e);
      }

      now = Time.monotonicNow();
      if (now < nextHB.get()) {
        if (!Thread.interrupted()) {
          try {
            Thread.sleep(nextHB.get() - now);
          } catch (InterruptedException e) {
            // TriggerHeartbeat is called during the sleep. Don't need to set
            // the interrupted state to true.
          }
        }
      }
    }

    // If we have got some exception in stateMachine we set the state to
    // shutdown to stop the stateMachine thread. Along with this we should
    // also stop the JobWorker.
    if (context.getShutdownOnError()) {
      LOG.error("JobworkerStateMachine Shutdown due to a critical error");
      jobworkerStopService.stopService();
    }
  }

  /**
   * Closes this stream and releases any system resources.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (stateMachineThread != null) {
      stateMachineThread.interrupt();
    }

    context.setState(JobworkerStates.getLastState());

    if (commandProcessor != null) {
      commandProcessor.close();
    }

    if (commandDispatcher != null) {
      commandDispatcher.stop();
    }

    if (executorService != null) {
      executorServiceShutdownGraceful(executorService);
    }

    if (connectionManager != null) {
      connectionManager.close();
    }

    if (volumeSet != null) {
      volumeSet.close();
    }
    if (reportManager != null) {
      reportManager.shutdown();
    }
  }

  /**
   * Stop the daemon thread of the jobworker state machine.
   */
  public synchronized void stopDaemon() {
    try {
      if (stateMachineThread != null) {
        LOG.info("Stopping JobWorker state machine...");
        context.setShutdownGracefully();
        context.setState(JobworkerStates.SHUTDOWN);
        // If we had a report manager, would shut it down here
        // reportManager.shutdown();
        this.close();
        LOG.info("JobWorker service stopped.");

        if (reportManager != null) {
          reportManager.shutdown();
        }
      }
    } catch (IOException e) {
      LOG.error("Stop JobWorker service failed.", e);
    }
  }

  /**
   * Start jobworker state machine as a single thread daemon.
   */
  public void startDaemon() {
    Runnable startStateMachineTask = () -> {
      try {
        LOG.info("JobWorker service starting...");
        startStateMachineThread();
        LOG.info("JobWorker service started successfully.");
      } catch (Exception ex) {
        LOG.error("Unable to start the JobWorkerStateMachine", ex);
      }
    };
    stateMachineThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(context.getThreadNamePrefix() +
            "-StateMachineDaemonThread")
        .setUncaughtExceptionHandler((Thread t, Throwable ex) -> {
          String message = "Terminate JobWorker, encountered uncaught exception"
              + " in JobWorker State Machine Thread";
          LOG.error(message, ex);
          jobworkerStopService.stopService();
        })
        .build().newThread(startStateMachineTask);
    stateMachineThread.start();
  }

  /**
   * Waits for JobWorkerStateMachine to exit.
   */
  public void join() throws InterruptedException {
    if (stateMachineThread != null) {
      stateMachineThread.join();
    }
  }

  public JobworkerReportManager getReportManager() {
    return reportManager;
  }

  /**
   * Check if the daemon has been stopped.
   *
   * @return true if daemon has been stopped
   */
  @VisibleForTesting
  public boolean isDaemonStopped() {
    return this.executorService.isShutdown()
        && this.getContext().getState() == JobworkerStates.SHUTDOWN;
  }

  @VisibleForTesting
  public boolean isJobworkerReady() {
    if (context.getState() != JobworkerStates.RUNNING) {
      return false;
    }
    for (JobworkerEndpointStateMachine endpoint : connectionManager.getAllEndpoints()) {
      if (endpoint.getState() != EndpointStates.HEARTBEAT) {
        return false;
      }
    }
    return true;
  }

}
