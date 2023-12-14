/*
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
package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.scm.server.ContainerReportQueue;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;

import java.util.List;

/**
 * Customized queue to handle FCR and ICR from datanode optimally,
 * avoiding duplicate FCR reports.
 */
public class ReconContainerReportQueue extends ContainerReportQueue {

  public ReconContainerReportQueue(int queueSize) {
    super(queueSize);
  }

  @Override
  protected boolean mergeIcr(ContainerReport val,
                             List<ContainerReport> dataList) {
    if (!dataList.isEmpty()) {
      if (SCMDatanodeHeartbeatDispatcher.ContainerReportType.ICR
          == dataList.get(dataList.size() - 1).getType()) {
        dataList.get(dataList.size() - 1).mergeReport(val);
        return true;
      }
    }
    return false;
  }
}
