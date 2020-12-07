/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.recon.persistence;

import java.io.Serializable;

public class ContainerHistory implements Serializable {

  private long containerID;
  private String datanodeUuid;
  private String datanodeHost;
  private long firstReportTimestamp;
  private long lastReportTimestamp;

  public ContainerHistory(long containerId, String datanodeUuid,
      String datanodeHost, long firstSeenTime, long lastSeenTime) {
    this.containerID = containerId;
    this.datanodeUuid = datanodeUuid;
    this.datanodeHost = datanodeHost;
    this.firstReportTimestamp = firstSeenTime;
    this.lastReportTimestamp = lastSeenTime;
  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public void setDatanodeUuid(String datanodeUuid) {
    this.datanodeUuid = datanodeUuid;
  }

  public String getDatanodeHost() {
    return datanodeHost;
  }

  public void setDatanodeHost(String datanodeHost) {
    this.datanodeHost = datanodeHost;
  }

  public long getFirstReportTimestamp() {
    return firstReportTimestamp;
  }

  public void setFirstReportTimestamp(long firstReportTimestamp) {
    this.firstReportTimestamp = firstReportTimestamp;
  }

  public long getLastReportTimestamp() {
    return lastReportTimestamp;
  }

  public void setLastReportTimestamp(long lastReportTimestamp) {
    this.lastReportTimestamp = lastReportTimestamp;
  }
}
