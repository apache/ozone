/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.DiskBalancerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DiskBalancer Reports from datanode.
 */
public class DiskBalancerReportHandler implements
    EventHandler<DiskBalancerReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DiskBalancerReportHandler.class);

  private DiskBalancerManager diskBalancerManager;

  public DiskBalancerReportHandler(DiskBalancerManager diskBalancerManager) {
    this.diskBalancerManager = diskBalancerManager;
  }

  @Override
  public void onMessage(DiskBalancerReportFromDatanode reportFromDatanode,
      EventPublisher publisher) {
    Preconditions.checkNotNull(reportFromDatanode);
    DatanodeDetails dn = reportFromDatanode.getDatanodeDetails();
    DiskBalancerReportProto diskBalancerReportProto =
        reportFromDatanode.getReport();
    Preconditions.checkNotNull(dn,
        "DiskBalancer Report is missing DatanodeDetails.");
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Processing diskBalancer report for dn: {}", dn);
    }
    try {
      diskBalancerManager.processDiskBalancerReport(
          diskBalancerReportProto, dn);
    } catch (Exception e) {
      LOGGER.error("Failed to process diskBalancer report={} from dn={}.",
          diskBalancerReportProto, dn, e);
    }
  }
}
