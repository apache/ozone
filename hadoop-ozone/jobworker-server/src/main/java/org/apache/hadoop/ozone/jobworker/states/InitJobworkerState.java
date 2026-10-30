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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.utils.JobworkerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Init Jobworker State is the task that gets run when we are in Init State.
 * It initializes connections to OzoneManager.
 */
public class InitJobworkerState implements JobworkerStateHandler<JobworkerStates>,
    Callable<JobworkerStates> {

  static final Logger LOG = LoggerFactory.getLogger(InitJobworkerState.class);

  private final JobworkerConnectionManager connectionManager;
  private final ConfigurationSource conf;
  private final JobworkerStateContext context;
  private Future<JobworkerStates> result;

  /**
   * Create InitJobworkerState Task.
   *
   * @param conf              Configuration
   * @param connectionManager Connection Manager
   * @param context           Current Context
   */
  public InitJobworkerState(ConfigurationSource conf,
                            JobworkerConnectionManager connectionManager,
                            JobworkerStateContext context) {
    this.conf = conf;
    this.connectionManager = connectionManager;
    this.context = context;
  }

  /**
   * Initializes the Jobworker by connecting to OzoneManager endpoints.
   *
   * @return next state
   */
  @Override
  public JobworkerStates call() throws Exception {
    try {
      Map<String, List<InetSocketAddress>> oMsAddress = JobworkerUtils.getOMsAddressForJobworker(conf);
      if (oMsAddress.isEmpty()) {
        LOG.error("No OM endpoints are found in the configuration.");
        return JobworkerStates.SHUTDOWN;
      }

      boolean anyAdded = false;
      // This omServiceId is the omServiceId in the current jobworker configuration and
      // may be different from the omServiceId on the OM side.
      for (String configuredOmServiceId : oMsAddress.keySet()) {
        if (oMsAddress.get(configuredOmServiceId).isEmpty()) {
          LOG.error("No OM endpoints are found in the configuration for the OM serviceId {}.", configuredOmServiceId);
          return JobworkerStates.SHUTDOWN;
        }

        for (InetSocketAddress address : oMsAddress.get(configuredOmServiceId)) {
          if (address.isUnresolved()) {
            throw new IllegalStateException(
                String.format("omServiceID (in configuration) %s address (%s) can't be resolved.",
                    configuredOmServiceId, address));
          }
          connectionManager.addOMEndpoint(address, context.getThreadNamePrefix(), configuredOmServiceId);
          anyAdded = true;
          LOG.info("Added OM endpoint: {} in OM serviceId (in configuration): {}", address, configuredOmServiceId);
        }
      }

      if (!anyAdded) {
        LOG.error("Failed to add any OM endpoints.");
        return JobworkerStates.SHUTDOWN;
      }

      return this.context.getState().getNextState();
    } catch (Exception e) {
      LOG.error("Exception during OM address initialization: ", e);
      return JobworkerStates.SHUTDOWN;
    }
  }

  /**
   * Called before entering this state.
   */
  @Override
  public void onEnter() {
    LOG.info("Entering init jobworker state");
  }

  /**
   * Called After exiting this state.
   */
  @Override
  public void onExit() {
    LOG.info("Exiting init jobworker state");
  }

  /**
   * Executes one or more tasks that is needed by this state.
   *
   * @param executor -  ExecutorService
   */
  @Override
  public void execute(ExecutorService executor) {
    result = executor.submit(this);
  }

  /**
   * Wait for execute to finish.
   *
   * @param time     - Time
   * @param timeUnit - Unit of time.
   */
  @Override
  public JobworkerStates await(
      long time, TimeUnit timeUnit) throws InterruptedException,
      ExecutionException, TimeoutException {
    return result.get(time, timeUnit);
  }
}
