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
 * Runtime contract for local Ozone cluster commands.
 */
public interface LocalOzoneRuntime extends AutoCloseable {

  /**
   * Starts the local Ozone runtime.
   *
   * <p>Port and endpoint accessors return usable values after this method
   * completes successfully.</p>
   *
   * @throws Exception if the local runtime cannot be started
   */
  void start() throws Exception;

  /**
   * Returns the host name or address shown to users for connecting to this
   * local runtime.
   *
   * <p>This is a user-facing host and may differ from the bind host used by
   * individual services.</p>
   *
   * @return user-facing host name or address
   */
  String getDisplayHost();

  /**
   * Returns the SCM client port for this local runtime.
   *
   * @return SCM port
   */
  int getScmPort();

  /**
   * Returns the OM client port for this local runtime.
   *
   * @return OM port
   */
  int getOmPort();

  /**
   * Returns the S3 Gateway HTTP port for this local runtime.
   *
   * @return S3 Gateway port
   */
  int getS3gPort();

  /**
   * Returns the full S3 Gateway endpoint shown to users.
   *
   * @return S3 Gateway endpoint, including scheme, host, and port
   */
  String getS3Endpoint();

  /**
   * Stops the local runtime and releases resources created during startup.
   *
   * @throws Exception if shutdown fails
   */
  @Override
  void close() throws Exception;
}
