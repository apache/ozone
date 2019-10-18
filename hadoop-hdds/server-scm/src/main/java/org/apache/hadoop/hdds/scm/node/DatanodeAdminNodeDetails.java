/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used by the DatanodeAdminMonitor to track the state and
 * details for Datanode decommission and maintenance. It provides a wrapper
 * around a DatanodeDetails object adding some additional states and helper
 * methods related to the admin workflow.
 */
public class DatanodeAdminNodeDetails {
  private DatanodeDetails datanodeDetails;
  private long maintenanceEndTime;
  private DatanodeAdminMonitor.States currentState;
  private long enteredStateAt = 0;

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminNodeDetails.class);


  /**
   * Create a new object given the DatanodeDetails and the maintenance endtime.
   * @param dn The datanode going through the admin workflow
   * @param maintenanceEnd The number of hours from 'now', when maintenance
   *                       should end automatically. Passing zero indicates
   *                       indicates maintenance will never end automatically.
   */
  DatanodeAdminNodeDetails(DatanodeDetails dn,
      DatanodeAdminMonitor.States initialState, long maintenanceEnd) {
    datanodeDetails = dn;
    setMaintenanceEnd(maintenanceEnd);
    currentState = initialState;
    enteredStateAt = System.currentTimeMillis();
  }

  public boolean shouldMaintenanceEnd() {
    if (0 == maintenanceEndTime) {
      return false;
    }
    return System.currentTimeMillis() >= maintenanceEndTime;
  }

  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  /**
   * Get the current admin workflow state for this node.
   * @return The current Admin workflow state for this node
   */
  public DatanodeAdminMonitor.States getCurrentState() {
    return currentState;
  }

  /**
   * Set the number of hours after which maintenance should end. Passing zero
   * indicates maintenance will never end automatically. It is possible to pass
   * a negative number of hours can be passed for testing purposes.
   * @param hoursFromNow The number of hours from now when maintenance should
   *                     end, or zero for it to never end.
   */
  @VisibleForTesting
  public void setMaintenanceEnd(long hoursFromNow) {
    if (0 == hoursFromNow) {
      maintenanceEndTime = 0;
      return;
    }
    // Convert hours to ms
    long msFromNow = hoursFromNow * 60L * 60L * 1000L;
    maintenanceEndTime = System.currentTimeMillis() + msFromNow;
  }

  /**
   * Given the workflow stateMachine and the current node status
   * (DECOMMISSIONING or ENTERING_MAINTENANCE) move the node to the next
   * admin workflow state.
   * @param sm The stateMachine which controls the state flow
   * @param nodeOperationalState The current operational state for the node, eg
   *                             decommissioning or entering_maintenance
   * @return
   * @throws InvalidStateTransitionException
   */
  public DatanodeAdminMonitor.States transitionState(
      StateMachine<DatanodeAdminMonitor.States,
          DatanodeAdminMonitor.Transitions> sm,
      NodeOperationalState nodeOperationalState)
      throws InvalidStateTransitionException {

    DatanodeAdminMonitor.States newState = sm.getNextState(currentState,
        getTransition(nodeOperationalState));
    long currentTime = System.currentTimeMillis();
    LOG.info("Datanode {} moved from admin workflow state {} to {} after {} "+
        "seconds", datanodeDetails, currentState, newState,
        (currentTime - enteredStateAt)/1000L);
    currentState = newState;
    enteredStateAt = currentTime;
    return currentState;
  }

  private DatanodeAdminMonitor.Transitions getTransition(
      NodeOperationalState nodeState) {
    if (nodeState == NodeOperationalState.DECOMMISSIONED ||
        nodeState == NodeOperationalState.DECOMMISSIONING) {
      return DatanodeAdminMonitor.Transitions.COMPLETE_DECOM_STAGE;
    } else if (nodeState ==
        NodeOperationalState.ENTERING_MAINTENANCE ||
        nodeState == NodeOperationalState.IN_MAINTENANCE) {
      return DatanodeAdminMonitor.Transitions.COMPLETE_MAINT_STAGE;
    } else {
      return DatanodeAdminMonitor.Transitions.UNEXPECTED_NODE_STATE;
    }
  }

  /**
   * Matches only on the DatanodeDetails field, which compares only the UUID
   * of the node to determine of they are the same object or not.
   *
   * @param o The object to compare this with
   * @return True if the object match, otherwise false
   *
   */
  @Override
  public boolean equals(Object o) {
    return o instanceof DatanodeAdminNodeDetails &&
        datanodeDetails.equals(
            ((DatanodeAdminNodeDetails) o).getDatanodeDetails());
  }

  @Override
  public int hashCode() {
    return datanodeDetails.hashCode();
  }

}