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

package org.apache.hadoop.hdds.scm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;

/**
 * Assessment result for whether a datanode has sufficient space for container placement.
 *
 * Tracks data and metadata volume requirements separately and provides debugging
 * information about volumes with insufficient space.
 */
public class SCMDatanodeCapacityInfo {

  private final DatanodeDetails datanodeDetails;
  private final VolumeInfo dataVolumeInfo;
  private final VolumeInfo metaVolumeInfo;

  SCMDatanodeCapacityInfo(
      DatanodeDetails datanodeDetails, long requiredDataSize, long requiredMetadataSize) {
    this.datanodeDetails = datanodeDetails;
    this.dataVolumeInfo = new VolumeInfo(requiredDataSize);
    this.metaVolumeInfo = new VolumeInfo(requiredMetadataSize);
  }

  /** @return true if datanode has sufficient space for both data and metadata */
  public boolean hasEnoughSpace() {
    return dataVolumeInfo.hasEnoughSpace() && metaVolumeInfo.hasEnoughSpace();
  }

  public boolean hasEnoughDataSpace() {
    return dataVolumeInfo.hasEnoughSpace();
  }

  public void markEnoughSpaceFoundForData() {
    this.dataVolumeInfo.markEnoughSpaceFound();
  }

  public void updateMostAvailableSpaceForData(long mostSpaceAvailable) {
    this.dataVolumeInfo.updateMostAvailableSpace(mostSpaceAvailable);
  }

  public void addFullDataVolume(StorageReportProto report, long usableSpace) {
    this.dataVolumeInfo.addFullVolume(new FullVolume(report.getStorageUuid(), usableSpace));
  }

  public boolean hasEnoughMetaSpace() {
    return metaVolumeInfo.hasEnoughSpace();
  }

  public void markEnoughSpaceFoundForMeta() {
    this.metaVolumeInfo.markEnoughSpaceFound();
  }

  public void updateMostAvailableSpaceForMeta(long mostSpaceAvailable) {
    this.metaVolumeInfo.updateMostAvailableSpace(mostSpaceAvailable);
  }

  public void addFullMetaVolume(MetadataStorageReportProto report) {
    this.metaVolumeInfo.addFullVolume(new FullVolume(report.getStorageLocation(), report.getRemaining()));
  }

  /**
   * Gets a formatted message for logging capacity check failures.
   * @return human-readable message explaining why this datanode was rejected
   */
  public String getInsufficientSpaceMessage() {
    if (hasEnoughSpace()) {
      return String.format("Datanode %s has sufficient space (data: %d bytes required, metadata: %d bytes required)",
          datanodeDetails.getUuidString(), dataVolumeInfo.requiredSpace, metaVolumeInfo.requiredSpace);
    }

    if (!hasEnoughDataSpace()) {
      return String.format("Datanode %s has no volumes with enough space to allocate %d bytes for data. data=%s, metadata=%s",
          datanodeDetails.getUuidString(), dataVolumeInfo.requiredSpace, dataVolumeInfo, metaVolumeInfo);
    } else {
      return String.format("Datanode %s has no volumes with enough space to allocate %d bytes for metadata. data=%s, metadata=%s",
          datanodeDetails.getUuidString(), metaVolumeInfo.requiredSpace, dataVolumeInfo, metaVolumeInfo);
    }
  }

  @Override
  public String toString() {
    return "SCMDatanodeCapacityInfo{" +
        "datanode=" + datanodeDetails.getUuidString() +
        ", data=" + dataVolumeInfo +
        ", metadata=" + metaVolumeInfo +
        '}';
  }

  /** Volume with insufficient space. Used for debugging. */
  private static class FullVolume {
    private final String identifier;
    private final long availableSpace;

    FullVolume(String identifier, long availableSpace) {
      this.identifier = identifier;
      this.availableSpace = availableSpace;
    }

    @Override
    public String toString() {
      return identifier + ":" + availableSpace;
    }
  }

  /** Tracks space requirements and insufficient volumes for one storage type. */
  private static class VolumeInfo {

    private final long requiredSpace;

    private final List<FullVolume> fullVolumes = new ArrayList<>();
    private long mostAvailableSpace = Long.MIN_VALUE;
    private boolean hasEnoughSpace = false;

    VolumeInfo(long requiredSpace) {
      this.requiredSpace = requiredSpace;
    }

    public void addFullVolume(FullVolume volume) {
      fullVolumes.add(volume);
    }

    public void updateMostAvailableSpace(long space) {
      mostAvailableSpace = Math.max(mostAvailableSpace, space);
    }

    public void markEnoughSpaceFound() {
      this.hasEnoughSpace = true;
    }

    public boolean hasEnoughSpace() {
      return requiredSpace <= 0 || hasEnoughSpace;
    }

    public List<FullVolume> getFullVolumes() {
      return Collections.unmodifiableList(fullVolumes);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{required=").append(requiredSpace);

      if (!hasEnoughSpace()) {
        String volumeList = fullVolumes.stream()
            .map(FullVolume::toString)
            .collect(Collectors.joining(", "));

        sb.append(", fullVolumes=[").append(volumeList).append(']');
      }

      sb.append('}');
      return sb.toString();
    }
  }

}
