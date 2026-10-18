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

package org.apache.hadoop.ozone;

/**
 * Phases of datanode startup, in the order they are executed.
 */
public enum DatanodeStartupPhase {
  INITIALIZING("Initializing"),
  WAITING_FOR_SCM("Waiting for SCM"),
  PREPARING_VOLUMES("Preparing volumes"),
  LOADING_CONTAINERS("Loading containers"),
  CHECKING_VOLUMES("Checking volumes"),
  STARTING_SERVICES("Starting container services");

  private final String description;

  DatanodeStartupPhase(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
