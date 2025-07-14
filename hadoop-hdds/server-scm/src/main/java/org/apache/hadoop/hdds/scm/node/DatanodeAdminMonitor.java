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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;

/**
 * Interface used by the DatanodeAdminMonitor, which can be used to
 * decommission or recommission nodes and take them in and out of maintenance.
 */
public interface DatanodeAdminMonitor extends Runnable {

  void startMonitoring(DatanodeDetails dn);

  void stopMonitoring(DatanodeDetails dn);

  Set<DatanodeAdminMonitorImpl.TrackedNode> getTrackedNodes();

  void setMetrics(NodeDecommissionMetrics metrics);

  Map<String, List<ContainerID>> getContainersPendingReplication(DatanodeDetails dn)
      throws NodeNotFoundException;
}
