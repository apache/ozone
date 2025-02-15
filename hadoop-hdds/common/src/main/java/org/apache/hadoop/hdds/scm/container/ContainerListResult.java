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

package org.apache.hadoop.hdds.scm.container;

import java.util.List;

/**
 * Wrapper class for the result of listing containers with their total count.
 */
public class ContainerListResult {
  private final List<ContainerInfo> containerInfoList;
  private final long totalCount;

  /**
   * Constructs a new ContainerListResult.
   *
   * @param containerInfoList the list of containers
   * @param totalCount the total number of containers
   */
  public ContainerListResult(List<ContainerInfo> containerInfoList, long totalCount) {
    this.containerInfoList = containerInfoList;
    this.totalCount = totalCount;
  }

  /**
   * Gets the list of containers.
   *
   * @return the list of containers
   */
  public List<ContainerInfo> getContainerInfoList() {
    return containerInfoList;
  }

  /**
   * Gets the total count of containers.
   *
   * @return the total count of containers
   */
  public long getTotalCount() {
    return totalCount;
  }
}
