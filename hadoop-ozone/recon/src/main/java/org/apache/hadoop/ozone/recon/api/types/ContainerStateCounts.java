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

/**
 * Represents statistics related to containers in the Ozone cluster.
 */
public class ContainerStateCounts {

  private int totalContainerCount;
  private int missingContainerCount;
  private int openContainersCount;
  private int deletedContainersCount;

  public int getTotalContainerCount() {
    return totalContainerCount;
  }

  public void setTotalContainerCount(int totalContainerCount) {
    this.totalContainerCount = totalContainerCount;
  }

  public int getMissingContainerCount() {
    return missingContainerCount;
  }

  public void setMissingContainerCount(int missingContainerCount) {
    this.missingContainerCount = missingContainerCount;
  }

  public int getOpenContainersCount() {
    return openContainersCount;
  }

  public void setOpenContainersCount(int openContainersCount) {
    this.openContainersCount = openContainersCount;
  }

  public int getDeletedContainersCount() {
    return deletedContainersCount;
  }

  public void setDeletedContainersCount(int deletedContainersCount) {
    this.deletedContainersCount = deletedContainersCount;
  }
}
