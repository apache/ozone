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

package org.apache.hadoop.ozone.local;

/**
 * Runtime contract for {@code ozone local run}.
 *
 * <p>Implementations manage the lifecycle of a local Ozone cluster,
 * including starting services, waiting for readiness, and shutdown.</p>
 */
public interface LocalOzoneRuntime extends AutoCloseable {

  /**
   * Starts the local Ozone cluster.
   *
   * <p>This method blocks until all services are started and the
   * cluster is ready to accept requests.</p>
   *
   * @throws Exception if startup fails
   */
  void start() throws Exception;

  /**
   * Returns the display host for service endpoints.
   */
  String getDisplayHost();

  /**
   * Returns the number of running datanodes.
   */
  int getDatanodeCount();

  /**
   * Shuts down the local Ozone cluster.
   *
   * @throws Exception if shutdown fails
   */
  @Override
  void close() throws Exception;
}
