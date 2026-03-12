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

package org.apache.hadoop.ozone.container.common.states.datanode;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  // Since we connectionManager endpoints can be changed by reconfiguration
  // we should not rely on ConnectionManager#getValues being unchanged between
  // execute and await
  private int executingEndpointCount = 0;

  public RunningDatanodeState(ConfigurationSource conf,
      SCMConnectionManager connectionManager,
      StateContext context) {
    this.connectionManager = connectionManager;
    this.conf = conf;
    this.context = context;
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
    executingEndpointCount = 0;
    for (EndpointStateMachine endpoint : connectionManager.getValues()) {
      Callable<EndPointStates> endpointTask = buildEndPointTask(endpoint);
      if (endpointTask != null) {
        // Just do a timely wait. A slow EndpointStateMachine won't occupy
        // the thread in executor from DatanodeStateMachine for a long time,
        // so that it won't affect the communication between datanode and
        // other EndpointStateMachine.
        long heartbeatFrequency;
        if (endpoint.isPassive()) {
          heartbeatFrequency = context.getReconHeartbeatFrequency();
        } else {
          heartbeatFrequency = context.getHeartbeatFrequency();
        }
        ecs.submit(() -> {
          try {
            return endpoint.getExecutorService()
                .submit(endpointTask)
                .get(context.getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            TimeoutException timeoutEx = new TimeoutException("Timeout occurred on endpoint: " + endpoint.getAddress());
            timeoutEx.initCause(e);
            throw timeoutEx;
          }
        });
        executingEndpointCount++;
      } else {
        // This can happen if a task is taking more time than the timeOut
        // specified for the task in await, and when it is completed the task
        // has set the state to shut down, we may see the state as shutdown
        // here. So, we need to shut down DatanodeStateMachine.
        LOG.error("State is Shutdown in RunningDatanodeState");
        context.setState(DatanodeStateMachine.DatanodeStates.SHUTDOWN);
      }
    }
  }

  @VisibleForTesting
  public void setExecutorCompletionService(ExecutorCompletionService e) {
    this.ecs = e;
  }

  @VisibleForTesting
  public void setExecutingEndpointCount(int executingEndpointCount) {
    this.executingEndpointCount = executingEndpointCount;
  }

  private Callable<EndPointStates> buildEndPointTask(
      EndpointStateMachine endpoint) {
    switch (endpoint.getState()) {
    case GETVERSION:
      // set the next heartbeat time to current to avoid wait for next heartbeat as REGISTER can be triggered
      // immediately after GETVERSION
      context.getParent().setNextHB(Time.monotonicNow());
      return new VersionEndpointTask(endpoint, conf,
          context.getParent().getContainer());
    case REGISTER:
      return RegisterEndpointTask.newBuilder()
          .setConfig(conf)
          .setEndpointStateMachine(endpoint)
          .setContext(context)
          .setDatanodeDetails(context.getParent().getDatanodeDetails())
          .setOzoneContainer(context.getParent().getContainer())
          .build();
    case HEARTBEAT:
      return HeartbeatEndpointTask.newBuilder()
          .setConfig(conf)
          .setEndpointStateMachine(endpoint)
          .setDatanodeDetails(context.getParent().getDatanodeDetails())
          .setContext(context)
          .build();
    default:
      return null;
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
        Throwable cause = e.getCause();
        if (cause instanceof TimeoutException) {
          LOG.warn("Detected timeout: {}", cause.getMessage());
        } else {
          LOG.error("Error in executing end point task.", e);
        }
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
    int returned = 0;
    long durationMS = timeUnit.toMillis(duration);
    long timeLeft = durationMS;
    long startTime = Time.monotonicNow();
    List<Future<EndPointStates>> results = new LinkedList<>();

    while (returned < executingEndpointCount && timeLeft > 0) {
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

  @Override
  public void clear() {
    ecs = null;
  }
}
