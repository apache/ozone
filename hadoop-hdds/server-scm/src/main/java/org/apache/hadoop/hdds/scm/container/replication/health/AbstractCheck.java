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

package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;

/**
 * Abstract base class for Container Health Checks to extend.
 */
public abstract class AbstractCheck implements HealthCheck {

  private HealthCheck successor = null;

  /**
   * Handle the container and apply the given check to it. This may result in
   * commands getting generated to correct container states, or nothing
   * happening if the container is healthy.
   * @param request ContainerCheckRequest object representing the container
   * @return True if the request was handled or false if it was not and should
   *         be handled by the next handler in the chain.
   */
  @Override
  public boolean handleChain(ContainerCheckRequest request) {
    boolean result = handle(request);
    if (!result && successor != null) {
      return successor.handleChain(request);
    }
    return result;
  }

  /**
   * Add a subsequent HealthCheck that will be tried only if the current check
   * returns false. This allows handlers to be chained together, and each will
   * be tried in turn until one succeeds.
   * @param healthCheck The HealthCheck to add to the successor, which will be
   *                    tried if the current check returns false.
   * @return The passed in health check, so the next check in the chain can be
   *         easily added to the successor.
   */
  @Override
  public HealthCheck addNext(HealthCheck healthCheck) {
    successor = healthCheck;
    return healthCheck;
  }

  /**
   * Handle the container and apply the given check to it. This may result in
   * commands getting generated to correct container states, or nothing
   * happening if the container is healthy.
   * @param request ContainerCheckRequest object representing the container
   * @return True if the request was handled or false if it was not and should
   *         be handled by the next handler in the chain.
   */
  @Override
  public abstract boolean handle(ContainerCheckRequest request);

}
