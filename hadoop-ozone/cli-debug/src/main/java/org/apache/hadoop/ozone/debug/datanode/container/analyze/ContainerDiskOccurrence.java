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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import java.util.Objects;

/**
 * On-disk occurrence of a container directory on a DataNode volume.
 */
public final class ContainerDiskOccurrence {

  private final long containerId;
  private final String containerPath;
  private final long sizeBytes;
  private final boolean sizeKnown;
  private final ContainerDirectoryScanner.ContainerDiskScanStatus status;

  ContainerDiskOccurrence(long containerId, String containerPath, long sizeBytes,
      boolean sizeKnown, ContainerDirectoryScanner.ContainerDiskScanStatus status) {
    this.containerId = containerId;
    this.containerPath = Objects.requireNonNull(containerPath, "containerPath");
    this.sizeBytes = sizeBytes;
    this.sizeKnown = sizeKnown;
    this.status = Objects.requireNonNull(status, "status");
  }

  public long getContainerId() {
    return containerId;
  }

  public String getContainerPath() {
    return containerPath;
  }

  public long getSizeBytes() {
    return sizeBytes;
  }

  public boolean isSizeKnown() {
    return sizeKnown;
  }

  public ContainerDirectoryScanner.ContainerDiskScanStatus getStatus() {
    return status;
  }
}
