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
package org.apache.hadoop.ozone.container.common.states.datanode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
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

/**
 * Class that implements handshake with SCM.
 */
public class RunningDatanodeState implements DatanodeState {
  static final Logger
      LOG = LoggerFactory.getLogger(RunningDatanodeState.class);
  private final SCMConnectionManager connectionManager;
  private final ConfigurationSource conf;
  private final StateContext context;
  private CompletionService<EndPointStates> ecs;
  /** Cache the end point task per end point per end point state. */
  private Map<EndpointStateMachine, Map<EndPointStates,
      Callable<EndPointStates>>> endpointTasks;

  public RunningDatanodeState(ConfigurationSource conf,
      SCMConnectionManager connectionManager,
      StateContext context) {
    this.connectionManager = connectionManager;
    this.conf = conf;
    this.context = context;
    initEndPointTask();
  }

  /**
   * Initialize end point tasks corresponding to each end point,
   * each end point state.
   */
  private void initEndPointTask() {
    endpointTasks = new HashMap<>();
    for (EndpointStateMachine endpoint : connectionManager.getValues()) {
      EnumMap<EndPointStates, Callable<EndPointStates>> endpointTaskForState =
          new EnumMap<>(EndPointStates.class);

      for (EndPointStates state : EndPointStates.values()) {
        Callable<EndPointStates> endPointTask = null;
        switch (state) {
        case GETVERSION:
          endPointTask = new VersionEndpointTask(endpoint, conf,
              context.getParent().getContainer());
          break;
        case REGISTER:
          endPointTask = RegisterEndpointTask.newBuilder()
              .setConfig(conf)
              .setEndpointStateMachine(endpoint)
              .setContext(context)
              .setDatanodeDetails(context.getParent().getDatanodeDetails())
              .setOzoneContainer(context.getParent().getContainer())
              .build();
          break;
        case HEARTBEAT:
          endPointTask = HeartbeatEndpointTask.newBuilder()
              .setConfig(conf)
              .setEndpointStateMachine(endpoint)
              .setDatanodeDetails(context.getParent().getDatanodeDetails())
              .setContext(context)
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
    LOG.trace("Entering handshake task.");
  }

  /**
   * Called After exiting this state.
   */
  @Override
  public void onExit() {
    LOG.trace("Exiting handshake task.");
  }

  /**
   * Executes one or more tasks that is needed by this state.
   *
   * @param executor -  ExecutorService
   */
  @Override
  public void execute(ExecutorService executor) {
    ecs = new ExecutorCompletionService<>(executor);
    for (EndpointStateMachine endpoint : connectionManager.getValues()) {
      Callable<EndPointStates> endpointTask = getEndPointTask(endpoint);
      if (endpointTask != null) {
        // Just do a timely wait. A slow EndpointStateMachine won't occupy
        // the thread in executor from DatanodeStateMachine for a long time,
        // so that it won't affect the communication between datanode and
        // other EndpointStateMachine.
        ecs.submit(() -> endpoint.getExecutorService()
            .submit(endpointTask)
            .get(context.getHeartbeatFrequency(), TimeUnit.MILLISECONDS));
      } else {
        // This can happen if a task is taking more time than the timeOut
        // specified for the task in await, and when it is completed the task
        // has set the state to Shutdown, we may see the state as shutdown
        // here. So, we need to Shutdown DatanodeStateMachine.
        LOG.error("State is Shutdown in RunningDatanodeState");
        context.setState(DatanodeStateMachine.DatanodeStates.SHUTDOWN);
      }
    }
  }

  @VisibleForTesting
  public void setExecutorCompletionService(ExecutorCompletionService e) {
    this.ecs = e;
  }

  private Callable<EndPointStates> getEndPointTask(
      EndpointStateMachine endpoint) {
    if (endpointTasks.containsKey(endpoint)) {
      return endpointTasks.get(endpoint).get(endpoint.getState());
    } else {
      throw new IllegalArgumentException("Illegal endpoint: " + endpoint);
    }
  }

  /**
   * Computes the next state the container state machine must move to by looking
   * at all the state of endpoints.
   * <p>
   * if any endpoint state has moved to Shutdown, either we have an
   * unrecoverable error or we have been told to shutdown. Either case the
   * datanode state machine should move to Shutdown state, otherwise we
   * remain in the Running state.
   *
   * @return next container state.
   */
  private DatanodeStateMachine.DatanodeStates
      computeNextContainerState(
      List<Future<EndPointStates>> results) {
    for (Future<EndPointStates> state : results) {
      try {
        if (state.get() == EndPointStates.SHUTDOWN) {
          // if any endpoint tells us to shutdown we move to shutdown state.
          return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
        }
      } catch (InterruptedException e) {
        LOG.error("Error in executing end point task.", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("Error in executing end point task.", e);
      }
    }
    return DatanodeStateMachine.DatanodeStates.RUNNING;
  }

  /**
   * Wait for execute to finish.
   *
   * @param duration - Time
   * @param timeUnit - Unit of duration.
   */
  @Override
  public DatanodeStateMachine.DatanodeStates
      await(long duration, TimeUnit timeUnit)
      throws InterruptedException {
    int count = connectionManager.getValues().size();
    int returned = 0;
    long durationMS = timeUnit.toMillis(duration);
    long timeLeft = durationMS;
    long startTime = Time.monotonicNow();
    List<Future<EndPointStates>> results = new LinkedList<>();

    while (returned < count && timeLeft > 0) {
      Future<EndPointStates> result =
          ecs.poll(timeLeft, TimeUnit.MILLISECONDS);
      if (result != null) {
        results.add(result);
        returned++;
      }
      timeLeft = durationMS - (Time.monotonicNow() - startTime);
    }
    return computeNextContainerState(results);
  }
}
