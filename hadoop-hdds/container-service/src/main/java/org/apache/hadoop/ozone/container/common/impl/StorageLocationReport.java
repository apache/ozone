/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.StorageTypeProto;

import java.io.IOException;

/**
 * Storage location stats of datanodes that provide back store for containers.
 *
 */
public class StorageLocationReport {

  private final String id;
  private final boolean failed;
  private final long capacity;
  private final long scmUsed;
  private final long remaining;
  private final StorageType storageType;
  private final String storageLocation;

  private StorageLocationReport(String id, boolean failed, long capacity,
      long scmUsed, long remaining, StorageType storageType,
      String storageLocation) {
    this.id = id;
    this.failed = failed;
    this.capacity = capacity;
    this.scmUsed = scmUsed;
    this.remaining = remaining;
    this.storageType = storageType;
    this.storageLocation = storageLocation;
  }

  public String getId() {
    return id;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getScmUsed() {
    return scmUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public StorageType getStorageType() {
    return storageType;
  }


  private StorageTypeProto getStorageTypeProto() throws
      IOException {
    StorageTypeProto storageTypeProto;
    switch (getStorageType()) {
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
   * Returns the SCMStorageReport protoBuf message for the Storage Location
   * report.
   * @return SCMStorageReport
   * @throws IOException In case, the storage type specified is invalid.
   */
  public SCMStorageReport getProtoBufMessage() throws IOException{
    SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
    return srb.setStorageUuid(getId())
        .setCapacity(getCapacity())
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

  public static StorageLocationReport getFromProtobuf(SCMStorageReport report)
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
    return builder.build();
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
    private StorageType storageType;
    private String storageLocation;

    /**
     * Sets the storageId.
     *
     * @param id storageId
     * @return StorageLocationReport.Builder
     */
    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets whether the volume failed or not.
     *
     * @param failed whether volume failed or not
     * @return StorageLocationReport.Builder
     */
    public Builder setFailed(boolean failed) {
      this.failed = failed;
      return this;
    }

    /**
     * Sets the capacity of volume.
     *
     * @param capacity capacity
     * @return StorageLocationReport.Builder
     */
    public Builder setCapacity(long capacity) {
      this.capacity = capacity;
      return this;
    }
    /**
     * Sets the scmUsed Value.
     *
     * @param scmUsed storage space used by scm
     * @return StorageLocationReport.Builder
     */
    public Builder setScmUsed(long scmUsed) {
      this.scmUsed = scmUsed;
      return this;
    }

    /**
     * Sets the remaining free space value.
     *
     * @param remaining remaining free space
     * @return StorageLocationReport.Builder
     */
    public Builder setRemaining(long remaining) {
      this.remaining = remaining;
      return this;
    }

    /**
     * Sets the storageType.
     *
     * @param storageType type of the storage used
     * @return StorageLocationReport.Builder
     */
    public Builder setStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    /**
     * Sets the storageLocation.
     *
     * @param storageLocation location of the volume
     * @return StorageLocationReport.Builder
     */
    public Builder setStorageLocation(String storageLocation) {
      this.storageLocation = storageLocation;
      return this;
    }

    /**
     * Builds and returns StorageLocationReport instance.
     *
     * @return StorageLocationReport
     */
    public StorageLocationReport build() {
      return new StorageLocationReport(id, failed, capacity, scmUsed,
          remaining, storageType, storageLocation);
    }

  }

}
