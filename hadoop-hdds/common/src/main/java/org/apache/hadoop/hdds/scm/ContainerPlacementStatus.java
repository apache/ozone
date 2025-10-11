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

package org.apache.hadoop.hdds.scm;

/**
 * Interface to allow container placement status to be queried to ensure a
 * container meets its placement policy (number of racks etc).
 */
public interface ContainerPlacementStatus {

  /**
   * Returns a boolean indicating if the container replicas meet the desired
   * placement policy. That is, they are placed on a sufficient number of
   * racks, or node groups etc. This does not check if the container is over
   * or under replicated, as it is possible for a container to have enough
   * replicas and still not meet the placement rules.
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
   * under replicated. The container could also have sufficient replicas but
   * require more to make it meet the policy, as the existing replicas are not
   * placed correctly.
   * @return The number of additional replicas required, or zero.
   */
  int misReplicationCount();

  /**
   * The number of locations (eg racks, node groups) the container should be
   * placed on.
   * @return The expected Placement Count
   */
  int expectedPlacementCount();

  /**
   * The actual placement count, eg how many racks the container is currently
   * on.
   * @return The actual placement count.
   */
  int actualPlacementCount();

}
