/*
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
package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

/**
 * A context that contains useful information for request validator instances.
 */
public interface ValidationContext {

  /**
   * Gets the {@link LayoutVersionManager} of the service, so that a pre
   * finalization validation can check if the layout version it belongs to
   * is finalized already or not.
   *
   * @return the {@link LayoutVersionManager} of the service
   */
  LayoutVersionManager versionManager();

  /**
   * Provides the protocol version of the server side running.
   * @return the server side protocol version
   */
  int serverVersion();

  /**
   * Creates a context object based on the given parameters.
   *
   * @param versionManager the {@link LayoutVersionManager} of the service
   * @param serverVersion the server side protocol version
   * @return the {@link ValidationContext} specified by the parameters.
   */
  static ValidationContext of(
      LayoutVersionManager versionManager,
      int serverVersion) {
    return new ValidationContext() {
      @Override
      public LayoutVersionManager versionManager() {
        return versionManager;
      }

      @Override
      public int serverVersion() {
        return serverVersion;
      }
    };
  }
}
