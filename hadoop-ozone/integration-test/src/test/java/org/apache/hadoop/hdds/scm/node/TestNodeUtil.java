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

package org.apache.hadoop.hdds.scm.node;

import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;

/**
 * Utility class with helper methods for testing node state and status.
 */
public final class TestNodeUtil {

  private TestNodeUtil() {
  }

  /**
   * Wait for the given datanode to reach the given operational state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public static void waitForDnToReachOpState(NodeManager nodeManager,
      DatanodeDetails dn, HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getNodeStatus(nodeManager, dn)
                  .getOperationalState().equals(state),
        200, 30000);
  }

  /**
   * Wait for the given datanode to reach the given Health state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public static void waitForDnToReachHealthState(NodeManager nodeManager,
      DatanodeDetails dn, HddsProtos.NodeState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getNodeStatus(nodeManager, dn).getHealth().equals(state),
        200, 30000);
  }

  /**
   * Retrieves the NodeStatus for the given DN or fails the test if the
   * Node cannot be found. This is a helper method to allow the nodeStatus to be
   * checked in lambda expressions.
   * @param dn Datanode for which to retrieve the NodeStatus.
   */
  public static NodeStatus getNodeStatus(NodeManager nodeManager,
      DatanodeDetails dn) {
    return Assertions.assertDoesNotThrow(
        () -> nodeManager.getNodeStatus(dn),
        "Unexpected exception getting the nodeState");
  }

  /**
   * Given a Datanode, return a string consisting of the hostname and one of its
   * ports in the for host:post.
   * @param dn Datanode for which to retrieve the host:post string
   * @return host:port for the given DN.
   */
  public static String getDNHostAndPort(DatanodeDetails dn) {
    return dn.getHostName() + ":" + dn.getPorts().get(0).getValue();
  }

  /**
   * Wait for the given datanode to reach the given persisted state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public static void waitForDnToReachPersistedOpState(DatanodeDetails dn,
      HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> dn.getPersistedOpState().equals(state),
        200, 30000);
  }
}
