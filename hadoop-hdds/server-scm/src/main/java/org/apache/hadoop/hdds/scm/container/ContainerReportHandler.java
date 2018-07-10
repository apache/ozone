/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Handles container reports from datanode.
 */
public class ContainerReportHandler implements
    EventHandler<ContainerReportFromDatanode> {

  private final Mapping containerMapping;
  private final Node2ContainerMap node2ContainerMap;

  public ContainerReportHandler(Mapping containerMapping,
                                Node2ContainerMap node2ContainerMap) {
    this.containerMapping = containerMapping;
    this.node2ContainerMap = node2ContainerMap;
  }

  @Override
  public void onMessage(ContainerReportFromDatanode containerReportFromDatanode,
                        EventPublisher publisher) {
    // TODO: process container report.
  }
}
