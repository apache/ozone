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

package org.apache.ozone.compaction.log;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.rocksdb.LiveFileMetaData;

/**
 * Dao to keep SST file information in the compaction log.
 */
public final class CompactionFileInfo extends SstFileInfo {
  private boolean pruned;

  @VisibleForTesting
  public CompactionFileInfo(String fileName,
                            String startRange,
                            String endRange,
                            String columnFamily) {
    this(fileName, startRange, endRange, columnFamily, false);
  }

  public CompactionFileInfo(String fileName,
                            String startRange,
                            String endRange,
                            String columnFamily,
                            boolean pruned) {
    super(fileName, startRange, endRange, columnFamily);
    this.pruned = pruned;
  }

  public boolean isPruned() {
    return pruned;
  }

  public void setPruned() {
    this.pruned = true;
  }

  public HddsProtos.CompactionFileInfoProto getProtobuf() {
    HddsProtos.CompactionFileInfoProto.Builder builder =
        HddsProtos.CompactionFileInfoProto.newBuilder()
            .setFileName(getFileName())
            .setPruned(pruned);
    if (getStartKey() != null) {
      builder = builder.setStartKey(getStartKey());
    }
    if (getEndKey() != null) {
      builder = builder.setEndKey(getEndKey());
    }
    if (getColumnFamily() != null) {
      builder = builder.setColumnFamily(getColumnFamily());
    }
    return builder.build();
  }

  public static CompactionFileInfo getFromProtobuf(
      HddsProtos.CompactionFileInfoProto proto) {
    Builder builder = new Builder(proto.getFileName());

    if (proto.hasStartKey()) {
      builder.setStartRange(proto.getStartKey());
    }
    if (proto.hasEndKey()) {
      builder.setEndRange(proto.getEndKey());
    }
    if (proto.hasColumnFamily()) {
      builder.setColumnFamily(proto.getColumnFamily());
    }
    if (proto.hasPruned() && proto.getPruned()) {
      builder.setPruned();
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return String.format("%s, isPruned: '%b'", super.toString(), pruned);
  }

  @Override
  public SstFileInfo copyObject() {
    return new CompactionFileInfo(getFileName(), getStartKey(), getEndKey(), getColumnFamily(), pruned);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CompactionFileInfo)) {
      return false;
    }
    return super.equals(o) && pruned == ((CompactionFileInfo)o).pruned;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pruned);
  }

  /**
   * Builder of CompactionFileInfo.
   */
  public static class Builder {
    private final String fileName;
    private String startRange;
    private String endRange;
    private String columnFamily;
    private boolean pruned = false;

    public Builder(String fileName) {
      this.fileName = Objects.requireNonNull(fileName, "FileName is required parameter.");
    }

    public Builder setStartRange(String startRange) {
      this.startRange = startRange;
      return this;
    }

    public Builder setEndRange(String endRange) {
      this.endRange = endRange;
      return this;
    }

    public Builder setColumnFamily(String columnFamily) {
      this.columnFamily = columnFamily;
      return this;
    }

    public Builder setValues(LiveFileMetaData fileMetaData) {
      if (fileMetaData != null) {
        String columnFamilyName = StringUtils.bytes2String(fileMetaData.columnFamilyName());
        String startRangeValue = StringUtils.bytes2String(fileMetaData.smallestKey());
        String endRangeValue = StringUtils.bytes2String(fileMetaData.largestKey());
        this.setColumnFamily(columnFamilyName).setStartRange(startRangeValue).setEndRange(endRangeValue);
      }
      return this;
    }

    public Builder setPruned() {
      this.pruned = true;
      return this;
    }

    public CompactionFileInfo build() {
      if ((startRange != null || endRange != null || columnFamily != null) &&
          (startRange == null || endRange == null || columnFamily == null)) {
        throw new IllegalArgumentException(
            String.format("Either all of startRange, endRange and " +
                    "columnFamily should be non-null or null. " +
                    "startRange: '%s', endRange: '%s', columnFamily: '%s'.",
                startRange, endRange, columnFamily));
      }

      return new CompactionFileInfo(fileName, startRange, endRange,
          columnFamily, pruned);
    }
  }
}
