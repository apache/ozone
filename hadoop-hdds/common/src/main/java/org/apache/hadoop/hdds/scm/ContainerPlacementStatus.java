/**
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

package org.apache.hadoop.hdds.scm;

/**
 * Interface to allow container placement status to be queried to ensure a
 * container meets its placement policy (number of racks etc).
 */
public interface ContainerPlacementStatus {

  /**
   * Returns a boolean indicating if the container replica meet the desired
   * replication policy.
   * @return True if the containers meet the policy. False otherwise.
   */
  boolean isPolicySatisfied();

  /**
   * Returns an String describing why a container does not meet the placement
   * policy.
   * @return String indicating the reason the policy is not satisfied or null if
   *         the policy is satisfied.
   */
  String misReplicatedReason();

  /**
   * If the container does not meet the placement policy, return an integer
   * indicating how many additional replicas are required so the container
   * meets the placement policy. Otherwise return zero.
   * Note the count returned are the number of replicas needed to meet the
   * placement policy. The container may need additional replicas if it is
   * under replicated.
   * @return The number of additional replicas required, or zero.
   */
  int additionalReplicaRequired();

}
