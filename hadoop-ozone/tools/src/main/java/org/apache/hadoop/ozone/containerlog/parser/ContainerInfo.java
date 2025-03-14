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

package org.apache.hadoop.ozone.containerlog.parser;

/**
 * Represents container information.
 */

public class ContainerInfo {

  private String containerFinalState;
  private long datanodeId;
  private long containerFinalBCSID;

  public ContainerInfo(String state, long dnodeId, long bcsid) {
    this.containerFinalState = state;
    this.datanodeId = dnodeId;
    this.containerFinalBCSID = bcsid;
  }

  public String getContainerFinalState() {
    return containerFinalState;
  }

  public void setContainerFinalState(String containerFinalState) {
    this.containerFinalState = containerFinalState;
  }

  public long getDatanodeId() {
    return datanodeId;
  }

  public void setDatanodeId(long datanodeId) {
    this.datanodeId = datanodeId;
  }

  public long getContainerFinalBCSID() {
    return containerFinalBCSID;
  }

  public void setContainerFinalBCSID(long containerFinalBCSID) {
    this.containerFinalBCSID = containerFinalBCSID;
  }

  @Override
  public String toString() {
    return "{" +
        "containerFinalState='" + containerFinalState + '\'' +
        ", datanodeId=" + datanodeId +
        ", containerFinalBCSID=" + containerFinalBCSID +
        '}';
  }
}
