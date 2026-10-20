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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommand;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandManager;
import org.apache.hadoop.ozone.jobworker.states.JobworkerStateHandler;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context of the Jobworker State Machine.
 */
public class JobworkerStateContext {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerStateContext.class);

  private final JobworkerCommandManager commandManager;
  private final JobworkerStateMachine parentJobworkerStateMachine;
  private final AtomicLong stateExecutionCount;
  private final JobworkerConfiguration jwConf;
  private final AtomicLong threadPoolNotAvailableCount;
  private final AtomicLong lastHeartbeatSent;
  private final long initializeHeartbeatFrequencyMs = 2000;
  private final AtomicLong heartbeatFrequencyMs = new AtomicLong(initializeHeartbeatFrequencyMs);
  private final String threadNamePrefix;
  private JobworkerStates state;
  private boolean shutdownOnError = false;
  private boolean shutdownGracefully = false;
  private final JobworkerDetails jobworkerDetails;

  /**
   * Constructs a StateContext for JobworkerStateMachine.
   *
   * @param conf             Configuration
   * @param state            Initial state
   * @param stateMachine           Parent state machine
   * @param jobworkerDetails  Details of this jobworker
   * @param threadNamePrefix Thread name prefix
   * @param commandManager   Command manager
   */
  public JobworkerStateContext(ConfigurationSource conf,
                               JobworkerStates state,
                               JobworkerDetails jobworkerDetails,
                               String threadNamePrefix,
                               JobworkerStateMachine stateMachine,
                               JobworkerCommandManager commandManager) {
    this.jwConf = conf.getObject(JobworkerConfiguration.class);
    this.state = state;
    this.parentJobworkerStateMachine = stateMachine;
    this.commandManager = commandManager;
    this.stateExecutionCount = new AtomicLong(0);
    this.threadPoolNotAvailableCount = new AtomicLong(0);
    this.lastHeartbeatSent = new AtomicLong(0);
    this.threadNamePrefix = threadNamePrefix;
    this.jobworkerDetails = jobworkerDetails;
  }

  /**
   * Returns the JobworkerStateMachine class that holds this state.
   *
   * @return JobworkerStateMachine
   */
  public JobworkerStateMachine getParent() {
    return parentJobworkerStateMachine;
  }

  public JobworkerDetails getJobworkerDetails() {
    return jobworkerDetails;
  }

  /**
   * Returns true if we are entering a new state.
   *
   * @return boolean
   */
  boolean isEntering() {
    return stateExecutionCount.get() == 0;
  }

  /**
   * Returns true if we are exiting from the current state.
   *
   * @param newState - newState.
   * @return boolean
   */
  boolean isExiting(JobworkerStates newState) {
    boolean isExiting = state != newState && stateExecutionCount.get() > 0;
    if (isExiting) {
      stateExecutionCount.set(0);
    }
    return isExiting;
  }

  /**
   * Returns the current state the machine is in.
   *
   * @return state.
   */
  public JobworkerStates getState() {
    return state;
  }

  /**
   * Sets the current state of the machine.
   *
   * @param state state.
   */
  public void setState(JobworkerStates state) {
    if (this.state != state) {
      if (this.state.isTransitionAllowedTo(state)) {
        this.state = state;
      } else {
        LOG.warn("Ignore disallowed transition from {} to {}",
            this.state, state);
      }
    }
  }

  /**
   * Sets the shutdownOnError. This method needs to be called when we
   * set JobworkerState to SHUTDOWN when executing a task of a JobworkerState.
   */
  void setShutdownOnError() {
    this.shutdownOnError = true;
  }

  /**
   * Indicate to the StateContext that StateMachine shutdown was called.
   */
  public void setShutdownGracefully() {
    this.shutdownGracefully = true;
  }

  /**
   * Get shutdownStateMachine.
   *
   * @return boolean
   */
  public boolean getShutdownOnError() {
    return shutdownOnError;
  }

  /**
   * Add a new endpoint to track.
   * This method is updated to register the endpoint with the report manager.
   */
  public void addEndpoint(InetSocketAddress endpoint, String omServiceId) {
    parentJobworkerStateMachine.getReportManager().registerEndpoint(endpoint, omServiceId);
  }

  /**
   * Get the command manager.
   *
   * @return command manager
   */
  public JobworkerCommandManager getCommandManager() {
    return commandManager;
  }

  /**
   * Returns the count of the Execution.
   *
   * @return long
   */
  public long getExecutionCount() {
    return stateExecutionCount.get();
  }

  /**
   * Configure heartbeat frequency from configuration.
   */
  public void configureHeartbeatFrequency() {
    heartbeatFrequencyMs.set(jwConf.getHeartbeatInterval().toMillis());
  }

  /**
   * Return current heartbeat frequency in ms.
   */
  public long getHeartbeatFrequencyMs() {
    return heartbeatFrequencyMs.get();
  }

  /**
   * Set heartbeat frequency manually.
   *
   * @param frequencyMs Frequency in milliseconds
   */
  public void configureHeartbeatHeartbeatFrequencyMs(long frequencyMs) {
    this.heartbeatFrequencyMs.set(frequencyMs);
  }

  /**
   * Return heartbeat intervals during initialization.
   */
  public long getInitializeHeartbeatFrequencyMs() {
    return initializeHeartbeatFrequencyMs;
  }

  /**
   * Get the thread name prefix.
   *
   * @return thread name prefix
   */
  public String getThreadNamePrefix() {
    return threadNamePrefix;
  }

  /**
   * Execute the required state function.
   *
   * @param service - Executor Service
   * @param time    - seconds to wait
   * @param unit    - Time unit
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public void execute(ExecutorService service, long time, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    stateExecutionCount.incrementAndGet();
    JobworkerStateHandler<JobworkerStates> task = getParent().getTask();

    // Adding not null check, in a case where jobworker is still starting up, but
    // we called stop JobworkerStateMachine, this sets state to SHUTDOWN, and
    // there is a chance of getting task as null.
    if (task == null) {
      return;
    }

    if (this.isEntering()) {
      task.onEnter();
    }

    boolean isThreadPoolAvailable = isThreadPoolAvailable(service);
    if (!isThreadPoolAvailable) {
      long count = threadPoolNotAvailableCount.incrementAndGet();
      long unavailableTime = Time.monotonicNow() - lastHeartbeatSent.get();
      if (unavailableTime > time && count % jwConf.getHeartbeatLogWarnInterval() == 0) {
        LOG.warn("No available thread in pool for the past {} seconds " +
            "and {} times.", unit.toSeconds(unavailableTime), count);
      }
      return;
    }
    threadPoolNotAvailableCount.set(0);

    task.execute(service);
    lastHeartbeatSent.set(Time.monotonicNow());
    JobworkerStates newState = task.await(time, unit);
    if (this.state != newState) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Task {} executed, state transited from {} to {}",
            task.getClass().getSimpleName(), this.state, newState);
      }
      if (isExiting(newState)) {
        task.onExit();
      }
      this.setState(newState);
    }

    if (!shutdownGracefully &&
        this.state == JobworkerStates.SHUTDOWN) {
      LOG.error("Critical error occurred in StateMachine, setting " +
          "shutDownMachine");
      // When some exception occurred, set shutdownStateMachine to true, so
      // that we can terminate the jobworker.
      setShutdownOnError();
    }
  }

  /**
   * Check if thread pool has available threads.
   *
   * @param executor The executor service
   * @return true if threads are available, false otherwise
   */
  private boolean isThreadPoolAvailable(ExecutorService executor) {
    if (executor instanceof java.util.concurrent.ThreadPoolExecutor) {
      java.util.concurrent.ThreadPoolExecutor ex =
          (java.util.concurrent.ThreadPoolExecutor) executor;
      return ex.getQueue().isEmpty();
    }
    return true;
  }

  /**
   * Update the term of leader OM for a specific service ID.
   * This is a convenience method that delegates to the command manager.
   *
   * @param omServiceId The OM service ID
   * @param newTerm     New term value
   */
  public void updateTermOfLeaderOM(String omServiceId, long newTerm) {
    commandManager.updateTermOfLeaderOM(omServiceId, newTerm);
  }

  /**
   * Adds a command to the command queue.
   * This is a convenience method that delegates to the command manager.
   *
   * @param command - JobworkerCommand
   */
  public void addCommand(JobworkerCommand command) {
    commandManager.addCommand(command);
  }

  public void putBackCommandStatusReports(String omServiceId, InetSocketAddress address,
                                          List<CommandStatusReportsProto> reports) {
    for (CommandStatusReportsProto report : reports) {
      parentJobworkerStateMachine.getReportManager()
          .addCommandStatusReport(omServiceId, address, report);
    }
  }
}
