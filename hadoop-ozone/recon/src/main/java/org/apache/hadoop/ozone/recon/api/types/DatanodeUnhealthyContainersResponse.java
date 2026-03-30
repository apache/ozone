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

package org.apache.hadoop.ozone.recon.api.types;

import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Response object for the drill-down of unhealthy containers
 * on a specific DataNode.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DatanodeUnhealthyContainersResponse {

  @XmlElement(name = "datanodeUuid")
  private String datanodeUuid;

  @XmlElement(name = "datanodeHost")
  private String datanodeHost;

  @XmlElement(name = "totalCount")
  private int totalCount;

  @XmlElement(name = "containers")
  private List<UnhealthyContainerMetadata> containers;

  public DatanodeUnhealthyContainersResponse() {
  }

  public DatanodeUnhealthyContainersResponse(
      String datanodeUuid, String datanodeHost,
      int totalCount, List<UnhealthyContainerMetadata> containers) {
    this.datanodeUuid = datanodeUuid;
    this.datanodeHost = datanodeHost;
    this.totalCount = totalCount;
    this.containers = containers;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public String getDatanodeHost() {
    return datanodeHost;
  }

  public int getTotalCount() {
    return totalCount;
  }

  public List<UnhealthyContainerMetadata> getContainers() {
    return containers;
  }
}
