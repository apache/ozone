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

import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Summary of unhealthy containers for a specific DataNode.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DatanodeUnhealthySummary {

  @XmlElement(name = "datanodeUuid")
  private String datanodeUuid;

  @XmlElement(name = "datanodeHost")
  private String datanodeHost;

  @XmlElement(name = "totalUnhealthyContainers")
  private int totalUnhealthyContainers;

  @XmlElement(name = "stateCounts")
  private Map<String, Integer> stateCounts;

  public DatanodeUnhealthySummary() {
    this.stateCounts = new HashMap<>();
  }

  public DatanodeUnhealthySummary(String datanodeUuid, String datanodeHost) {
    this.datanodeUuid = datanodeUuid;
    this.datanodeHost = datanodeHost;
    this.totalUnhealthyContainers = 0;
    this.stateCounts = new HashMap<>();
  }

  public void incrementStateCount(String state) {
    stateCounts.merge(state, 1, Integer::sum);
    totalUnhealthyContainers++;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public String getDatanodeHost() {
    return datanodeHost;
  }

  public int getTotalUnhealthyContainers() {
    return totalUnhealthyContainers;
  }

  public Map<String, Integer> getStateCounts() {
    return stateCounts;
  }
}
