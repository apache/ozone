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

package org.apache.hadoop.ozone.om.ha;

/**
 * Interface for stateful background service in OM.
 *
 * Provide a fine-grained method to manipulate the status of these background
 * services.
 */
public interface OMService {
  /**
   * Notify raft or safe mode related status changed.
   */
  void notifyStatusChanged();

  /**
   * @return true, if next iteration of Service should take effect,
   *         false, if next iteration of Service should be skipped.
   */
  boolean shouldRun();

  /**
   * @return name of the Service.
   */
  String getServiceName();

  /**
   * Status of Service.
   */
  enum ServiceStatus {
    RUNNING,
    PAUSING
  }

  /**
   * starts the OM service.
   */
  void start() throws OMServiceException;

  /**
   * stops the OM service.
   */
  void stop();

}
