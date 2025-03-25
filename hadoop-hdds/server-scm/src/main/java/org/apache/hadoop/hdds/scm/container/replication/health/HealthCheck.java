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
 * Interface used by Container Health Check Handlers.
 */
public interface HealthCheck {

  /**
   * Handle the container and apply the given check to it. This may result in
   * commands getting generated to correct container states, or nothing
   * happening if the container is healthy.
   * @param request ContainerCheckRequest object representing the container
   * @return True if the request was handled or false if it was not and should
   *         be handled by the next handler in the chain.
   */
  boolean handle(ContainerCheckRequest request);

  /**
   * Starting from this HealthCheck, call the handle method. If it returns
   * false, indicating the request was not handled, then forward the request to
   * the next handler in the chain via its handleChain method. Repeating until
   * the request is handled, or there are no further handlers to try.
   * @param request
   * @return True if the request was handled or false if not handler handled it.
   */
  boolean handleChain(ContainerCheckRequest request);

  /**
   * Add a subsequent HealthCheck that will be tried only if the current check
   * returns false. This allows handlers to be chained together, and each will
   * be tried in turn until one succeeds.
   * @param handler
   */
  HealthCheck addNext(HealthCheck handler);

}
