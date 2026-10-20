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

package org.apache.hadoop.ozone.jobworker.states;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.jobworker.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.jobworker.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements communication with OM.
 */
public class RunningJobworkerState implements JobworkerStateHandler<JobworkerStates> {

  private static final Logger LOG = LoggerFactory.getLogger(RunningJobworkerState.class);

  private final JobworkerConnectionManager connectionManager;
  private final ConfigurationSource conf;
  private final JobworkerStateContext context;
  private final JobworkerVolumeSet jobworkerVolumeSet;
  private CompletionService<EndpointStates> completionService;
  /**
   * Cache the endpoint task per endpoint per endpoint state.
   */
  private Map<JobworkerEndpointStateMachine, Map<EndpointStates, Callable<EndpointStates>>> endpointTasks;

  public RunningJobworkerState(ConfigurationSource conf,
                               JobworkerConnectionManager connectionManager,
                               JobworkerStateContext context,
                               JobworkerVolumeSet jobworkerVolumeSet) {
    this.connectionManager = connectionManager;
    this.conf = conf;
    this.context = context;
    this.jobworkerVolumeSet = jobworkerVolumeSet;
    initEndPointTask();
  }

  /**
   * Initialize endpoint tasks corresponding to each endpoint, each endpoint state.
   */
  private void initEndPointTask() {
    endpointTasks = new java.util.HashMap<>();
    for (JobworkerEndpointStateMachine endpoint : connectionManager.getAllEndpoints()) {
      java.util.EnumMap<EndpointStates, Callable<EndpointStates>> endpointTaskForState =
          new java.util.EnumMap<>(EndpointStates.class);

      for (EndpointStates state : EndpointStates.values()) {
        Callable<EndpointStates> endPointTask = null;
        switch (state) {
        case GETVERSION:
          endPointTask = new VersionEndpointTask(endpoint, jobworkerVolumeSet);
          break;
        case REGISTER:
          endPointTask = RegisterEndpointTask.newBuilder()
              .setConfig(conf)
              .setEndpointStateMachine(endpoint)
              .setContext(context)
              .setJobworkerDetails(context.getJobworkerDetails())
              .build();
          break;
        case HEARTBEAT:
          endPointTask = HeartbeatEndpointTask.newBuilder()
              .setContext(context)
              .setJobworkerDetails(context.getJobworkerDetails())
              .setEndpointStateMachine(endpoint)
              .build();
          break;
        default:
          break;
        }
        if (endPointTask != null) {
          endpointTaskForState.put(state, endPointTask);
        }
      }
      endpointTasks.put(endpoint, endpointTaskForState);
    }
  }

  /**
   * Called before entering this state.
   */
  @Override
  public void onEnter() {
    LOG.trace("Entering RunningJobworkerState");
  }

  /**
   * Called After exiting this state.
   */
  @Override
  public void onExit() {
    LOG.trace("Exiting RunningJobworkerState");
  }

  /**
   * Executes one or more tasks that are needed by this state.
   *
   * @param executor - ExecutorService
   */
  @Override
  public void execute(ExecutorService executor) {
    completionService = new ExecutorCompletionService<>(executor);

    for (JobworkerEndpointStateMachine endpoint : connectionManager.getAllEndpoints()) {
      Callable<EndpointStates> endpointTask = getEndPointTask(endpoint);
      if (endpointTask != null) {
        // Just do a timely wait. A slow EndpointStateMachine won't occupy
        // the thread in executor for a long time, so it won't affect the
        // communication between jobworker and other EndpointStateMachine.
        long heartbeatFrequency = context.getHeartbeatFrequencyMs();
        completionService.submit(() -> endpoint.getExecutorService()
            .submit(endpointTask)
            .get(heartbeatFrequency, TimeUnit.MILLISECONDS));
      } else {
        // This can happen if a task is taking more time than the timeout
        // specified for the task in await, and when it is completed the task
        // has set the state to shut down, we may see the state as shutdown
        // here. So, we need to shut down JobworkerStateMachine.
        LOG.error("State is Shutdown in RunningJobworkerState");
        context.setState(JobworkerStates.SHUTDOWN);
      }
    }
  }

  @VisibleForTesting
  public void setExecutorCompletionService(ExecutorCompletionService<EndpointStates> ecs) {
    this.completionService = ecs;
  }

  private Callable<EndpointStates> getEndPointTask(JobworkerEndpointStateMachine endpoint) {
    if (endpointTasks.containsKey(endpoint)) {
      return endpointTasks.get(endpoint).get(endpoint.getState());
    } else {
      throw new IllegalArgumentException("Illegal endpoint: " + endpoint);
    }
  }

  /**
   * Computes the next state the jobworker state machine must move to by looking
   * at all the states of endpoints.
   * <p>
   * If any endpoint state has moved to Shutdown, either we have an
   * unrecoverable error or we have been told to shutdown. Either case the
   * jobworker state machine should move to Shutdown state, otherwise we
   * remain in the Running state.
   *
   * @return next jobworker state
   */
  private JobworkerStates computeNextJobworkerState(
      List<Future<EndpointStates>> results) {
    for (Future<EndpointStates> state : results) {
      try {
        if (state.get() == EndpointStates.SHUTDOWN) {
          // if any endpoint tells us to shutdown we move to shutdown state.
          return JobworkerStates.SHUTDOWN;
        }
      } catch (InterruptedException e) {
        LOG.error("Error in executing endpoint task.", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("Error in executing endpoint task.", e);
      }
    }
    return JobworkerStates.RUNNING;
  }

  /**
   * Wait for executing to finish.
   *
   * @param duration - Time
   * @param timeUnit - Unit of duration
   */
  @Override
  public JobworkerStates await(long duration, TimeUnit timeUnit)
      throws InterruptedException {
    int count = connectionManager.getAllEndpoints().size();
    int returned = 0;
    long durationMS = timeUnit.toMillis(duration);
    long timeLeft = durationMS;
    long startTime = Time.monotonicNow();
    List<Future<EndpointStates>> results = new LinkedList<>();

    while (returned < count && timeLeft > 0) {
      Future<EndpointStates> result = completionService.poll(timeLeft, TimeUnit.MILLISECONDS);
      if (result != null) {
        results.add(result);
        returned++;
      }
      timeLeft = durationMS - (Time.monotonicNow() - startTime);
    }

    return computeNextJobworkerState(results);
  }
}
