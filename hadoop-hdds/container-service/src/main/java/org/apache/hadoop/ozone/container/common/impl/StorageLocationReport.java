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

package org.apache.hadoop.ozone.container.common.impl;

import java.io.IOException;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.ozone.container.common.interfaces.StorageLocationReportMXBean;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;

/**
 * Storage location stats of datanodes that provide back store for containers.
 *
 */
@Immutable
public final class StorageLocationReport implements StorageLocationReportMXBean {

  private final String id;
  private final boolean failed;
  private final long capacity;
  private final long scmUsed;
  private final long remaining;
  private final long committed;
  private final long freeSpaceToSpare;
  private final StorageType storageType;
  private final String storageLocation;
  private final long reserved;

  private StorageLocationReport(Builder builder) {
    this.id = builder.id;
    this.failed = builder.failed;
    this.capacity = builder.capacity;
    this.scmUsed = builder.scmUsed;
    this.remaining = builder.remaining;
    this.committed = builder.committed;
    this.freeSpaceToSpare = builder.freeSpaceToSpare;
    this.storageType = builder.storageType;
    this.storageLocation = builder.storageLocation;
    this.reserved = builder.reserved;
  }

  public long getUsableSpace() {
    return VolumeUsage.getUsableSpace(this);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public boolean isFailed() {
    return failed;
  }

  @Override
  public long getCapacity() {
    return capacity;
  }

  @Override
  public long getScmUsed() {
    return scmUsed;
  }

  @Override
  public long getRemaining() {
    return remaining;
  }

  @Override
  public long getCommitted() {
    return committed;
  }

  @Override
  public long getFreeSpaceToSpare() {
    return freeSpaceToSpare;
  }

  @Override
  public String getStorageLocation() {
    return storageLocation;
  }

  @Override
  public String getStorageTypeName() {
    return storageType.name();
  }

  public StorageType getStorageType() {
    return storageType;
  }

  private StorageTypeProto getStorageTypeProto() throws IOException {
    return getStorageTypeProto(getStorageType());
  }

  public static StorageTypeProto getStorageTypeProto(StorageType type)
      throws IOException {
    StorageTypeProto storageTypeProto;
    switch (type) {
    case SSD:
      storageTypeProto = StorageTypeProto.SSD;
      break;
    case DISK:
      storageTypeProto = StorageTypeProto.DISK;
      break;
    case ARCHIVE:
      storageTypeProto = StorageTypeProto.ARCHIVE;
      break;
    case PROVIDED:
      storageTypeProto = StorageTypeProto.PROVIDED;
      break;
    case RAM_DISK:
      storageTypeProto = StorageTypeProto.RAM_DISK;
      break;
    default:
      throw new IOException("Illegal Storage Type specified");
    }
    return storageTypeProto;
  }

  public long getReserved() { 
    return reserved;
  }

  private static StorageType getStorageType(StorageTypeProto proto) throws
      IOException {
    StorageType storageType;
    switch (proto) {
    case SSD:
      storageType = StorageType.SSD;
      break;
    case DISK:
      storageType = StorageType.DISK;
      break;
    case ARCHIVE:
      storageType = StorageType.ARCHIVE;
      break;
    case PROVIDED:
      storageType = StorageType.PROVIDED;
      break;
    case RAM_DISK:
      storageType = StorageType.RAM_DISK;
      break;
    default:
      throw new IOException("Illegal Storage Type specified");
    }
    return storageType;
  }

  /**
   * Returns the StorageReportProto protoBuf message for the Storage Location
   * report.
   * @return StorageReportProto
   * @throws IOException In case, the storage type specified is invalid.
   */
  public StorageReportProto getProtoBufMessage() throws IOException {
    StorageReportProto.Builder srb = StorageReportProto.newBuilder();
    return srb.setStorageUuid(getId())
        .setCapacity(getCapacity())
        .setScmUsed(getScmUsed())
        .setRemaining(getRemaining())
        .setCommitted(getCommitted())
        .setStorageType(getStorageTypeProto())
        .setStorageLocation(getStorageLocation())
        .setFailed(isFailed())
        .setFreeSpaceToSpare(getFreeSpaceToSpare())
        .setReserved(getReserved())
        .build();
  }

  /**
   * Returns the MetadataStorageReportProto protoBuf message for the
   * Storage Location report.
   * @return MetadataStorageReportProto
   * @throws IOException In case, the storage type specified is invalid.
   */
  public MetadataStorageReportProto getMetadataProtoBufMessage()
      throws IOException {
    MetadataStorageReportProto.Builder srb =
        MetadataStorageReportProto.newBuilder();
    return srb.setCapacity(getCapacity())
        .setScmUsed(getScmUsed())
        .setRemaining(getRemaining())
        .setStorageType(getStorageTypeProto())
        .setStorageLocation(getStorageLocation())
        .setFailed(isFailed())
        .build();
  }

  /**
   * Returns the StorageLocationReport from the protoBuf message.
   * @param report SCMStorageReport
   * @return StorageLocationReport
   * @throws IOException in case of invalid storage type
   */

  public static StorageLocationReport getFromProtobuf(StorageReportProto report)
      throws IOException {
    StorageLocationReport.Builder builder = StorageLocationReport.newBuilder();
    builder.setId(report.getStorageUuid())
        .setStorageLocation(report.getStorageLocation());
    if (report.hasCapacity()) {
      builder.setCapacity(report.getCapacity());
    }
    if (report.hasScmUsed()) {
      builder.setScmUsed(report.getScmUsed());
    }
    if (report.hasStorageType()) {
      builder.setStorageType(getStorageType(report.getStorageType()));
    }
    if (report.hasRemaining()) {
      builder.setRemaining(report.getRemaining());
    }

    if (report.hasFailed()) {
      builder.setFailed(report.getFailed());
    }

    if (report.hasReserved()) {
      builder.setReserved(report.getReserved());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128)
        .append('{')
        .append(" id=").append(id)
        .append(" dir=").append(storageLocation)
        .append(" type=").append(storageType);

    if (failed) {
      sb.append(" failed");
    } else {
      sb.append(" capacity=").append(capacity)
          .append(" used=").append(scmUsed)
          .append(" available=").append(remaining)
          .append(" minFree=").append(freeSpaceToSpare)
          .append(" committed=").append(committed);
    }

    return sb.append(" }").toString();
  }

  /**
   * Returns StorageLocation.Builder instance.
   *
   * @return StorageLocation.Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for building StorageLocationReport.
   */
  public static class Builder {
    private String id;
    private boolean failed;
    private long capacity;
    private long scmUsed;
    private long remaining;
    private long committed;
    private long freeSpaceToSpare;
    private StorageType storageType;
    private String storageLocation;
    private long reserved;

    /**
     * Sets the storageId.
     *
     * @param idValue storageId
     * @return StorageLocationReport.Builder
     */
    public Builder setId(String idValue) {
      this.id = idValue;
      return this;
    }

    /**
     * Sets whether the volume failed or not.
     *
     * @param failedValue whether volume failed or not
     * @return StorageLocationReport.Builder
     */
    public Builder setFailed(boolean failedValue) {
      this.failed = failedValue;
      return this;
    }

    public boolean isFailed() {
      return failed;
    }

    /**
     * Sets the capacity of volume.
     *
     * @param capacityValue capacity
     * @return StorageLocationReport.Builder
     */
    public Builder setCapacity(long capacityValue) {
      this.capacity = capacityValue;
      return this;
    }

    public long getCapacity() {
      return capacity;
    }

    /**
     * Sets the scmUsed Value.
     *
     * @param scmUsedValue storage space used by scm
     * @return StorageLocationReport.Builder
     */
    public Builder setScmUsed(long scmUsedValue) {
      this.scmUsed = scmUsedValue;
      return this;
    }

    /**
     * Sets the remaining free space value.
     *
     * @param remainingValue remaining free space
     * @return StorageLocationReport.Builder
     */
    public Builder setRemaining(long remainingValue) {
      this.remaining = remainingValue;
      return this;
    }

    /**
     * Sets the storageType.
     *
     * @param storageTypeValue type of the storage used
     * @return StorageLocationReport.Builder
     */
    public Builder setStorageType(StorageType storageTypeValue) {
      this.storageType = storageTypeValue;
      return this;
    }

    /**
     * Sets the committed bytes count.
     * (bytes for previously created containers)
     * @param committed previously created containers size
     * @return StorageLocationReport.Builder
     */
    public Builder setCommitted(long committed) {
      this.committed = committed;
      return this;
    }

    /**
     * Sets the free space available to spare.
     * (depends on datanode volume config,
     * consider 'hdds.datanode.volume.min.*' configuration properties)
     * @param freeSpaceToSpare the size of free volume space available to spare
     * @return StorageLocationReport.Builder
     */
    public Builder setFreeSpaceToSpare(long freeSpaceToSpare) {
      this.freeSpaceToSpare = freeSpaceToSpare;
      return this;
    }

    /**
     * Sets the storageLocation.
     *
     * @param storageLocationValue location of the volume
     * @return StorageLocationReport.Builder
     */
    public Builder setStorageLocation(String storageLocationValue) {
      this.storageLocation = storageLocationValue;
      return this;
    }

    public Builder setReserved(long reserved) { 
      this.reserved = reserved; 
      return this; 
    }

    public long getReserved() { 
      return reserved;
    }

    /**
     * Builds and returns StorageLocationReport instance.
     *
     * @return StorageLocationReport
     */
    public StorageLocationReport build() {
      return new StorageLocationReport(this);
    }

  }

}
