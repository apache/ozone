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

package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Stop reason codes and messages for ContainerBalancer.
 */
public enum ContainerBalancerStopReason {
  USER_REQUESTED("Stopped by user request."),
  SCM_STATE_CHANGE("Stopped because SCM state changed."),
  COMPLETED_ALL_ITERATIONS("Completed all configured number of iterations."),
  CAN_NOT_BALANCE_ANY_MORE("No more eligible container moves were found."),
  INITIALIZATION_FAILED("Failed to initialize a container balancer iteration."),
  ERROR("Stopped because of an unexpected error."),
  UNKNOWN("Stopped for an unknown reason.");
  
  public static final String INIT_SCM_NOT_READY = "SCM is in safe mode or is not leader ready.";
  public static final String INIT_EMPTY_DATANODE_LIST = "Received an empty list of Datanodes from Node Manager.";
  public static final String INIT_NO_UNBALANCED_DATANODES = "Did not find any unbalanced Datanodes.";

  private final String message;

  ContainerBalancerStopReason(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public String formatMessage(String details) {
    if (details == null || details.isEmpty()) {
      return message;
    }
    return message + " Details: " + details;
  }

  public static String exceptionDetails(Throwable throwable) {
    if (throwable == null) {
      return "";
    }
    String exceptionMessage = throwable.getMessage();
    if (exceptionMessage != null && !exceptionMessage.isEmpty()) {
      return throwable.getClass().getName() + ": " + exceptionMessage;
    }
    return throwable.toString();
  }
}
