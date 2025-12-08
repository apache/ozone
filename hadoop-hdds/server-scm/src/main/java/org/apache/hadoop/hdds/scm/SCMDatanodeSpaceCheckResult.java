package org.apache.hadoop.hdds.scm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;

public class SCMDatanodeSpaceCheckResult {

  // Input parameters
  private final DatanodeDetails datanodeDetails;
  private final long dataSizeRequired;
  private final long metadataSizeRequired;

  // Core results
  private final boolean hasEnoughDataSpace;
  private final boolean hasEnoughMetaSpace;

  // Diagnostic info - track insufficient volumes like AvailableSpaceFilter
  private final List<StorageReportProto> insufficientDataVolumes = new ArrayList<>();
  private final List<MetadataStorageReportProto> insufficientMetaVolumes = new ArrayList<>();
  private final long mostAvailableDataSpace;
  private final long mostAvailableMetaSpace;

  SCMDatanodeSpaceCheckResult(DatanodeDetails datanodeDetails,
                           long dataSizeRequired,
                           long metadataSizeRequired,
                           boolean hasEnoughDataSpace,
                           boolean hasEnoughMetaSpace,
                           List<StorageReportProto> insufficientDataVolumes,
                           List<MetadataStorageReportProto> insufficientMetaVolumes,
                           long mostAvailableDataSpace,
                           long mostAvailableMetaSpace) {
    this.datanodeDetails = datanodeDetails;
    this.dataSizeRequired = dataSizeRequired;
    this.metadataSizeRequired = metadataSizeRequired;
    this.hasEnoughDataSpace = hasEnoughDataSpace;
    this.hasEnoughMetaSpace = hasEnoughMetaSpace;
    this.insufficientDataVolumes.addAll(insufficientDataVolumes);
    this.insufficientMetaVolumes.addAll(insufficientMetaVolumes);
    this.mostAvailableDataSpace = mostAvailableDataSpace;
    this.mostAvailableMetaSpace = mostAvailableMetaSpace;
  }

  public boolean hasEnoughSpace() {
    return hasEnoughDataSpace && hasEnoughMetaSpace;
  }

  public boolean hasEnoughDataSpace() {
    return hasEnoughDataSpace;
  }

  public boolean hasEnoughMetaSpace() {
    return hasEnoughMetaSpace;
  }

  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  public List<StorageReportProto> getInsufficientDataVolumes() {
    return Collections.unmodifiableList(insufficientDataVolumes);
  }

  public List<MetadataStorageReportProto> getInsufficientMetaVolumes() {
    return Collections.unmodifiableList(insufficientMetaVolumes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("datanode: ").append(datanodeDetails)
        .append(", data required: ").append(dataSizeRequired)
        .append(", metadata required: ").append(metadataSizeRequired);

    if (!insufficientDataVolumes.isEmpty()) {
      sb.append(", insufficient data volumes: ").append(insufficientDataVolumes);
    }

    if (!insufficientMetaVolumes.isEmpty()) {
      sb.append(", insufficient metadata volumes: ").append(insufficientMetaVolumes);
    }

    if (hasEnoughSpace()) {
      sb.append(" [SUITABLE]");
    } else {
      sb.append(" [REJECTED]");
    }

    return sb.toString();
  }

}
