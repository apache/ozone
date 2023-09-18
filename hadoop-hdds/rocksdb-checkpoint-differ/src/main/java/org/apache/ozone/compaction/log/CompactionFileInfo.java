/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.compaction.log;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.util.Preconditions;

/**
 * Dao to keep SST file information in the compaction log.
 */
public final class CompactionFileInfo {
  private final String fileName;
  private final String startKey;
  private final String endKey;
  private final String columnFamily;

  private CompactionFileInfo(String fileName,
                             String startRange,
                             String endRange,
                             String columnFamily) {
    this.fileName = fileName;
    this.startKey = startRange;
    this.endKey = endRange;
    this.columnFamily = columnFamily;
  }

  public String getFileName() {
    return fileName;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public HddsProtos.CompactionFileInfoProto getProtobuf() {
    HddsProtos.CompactionFileInfoProto.Builder builder =
        HddsProtos.CompactionFileInfoProto.newBuilder()
            .setFileName(fileName);
    if (startKey != null) {
      builder = builder.setStartKey(startKey);
    }
    if (endKey != null) {
      builder = builder.setStartKey(endKey);
    }
    if (columnFamily != null) {
      builder = builder.setStartKey(columnFamily);
    }
    return builder.build();
  }

  public static CompactionFileInfo getFromProtobuf(
      HddsProtos.CompactionFileInfoProto proto) {
    return new CompactionFileInfo(proto.getFileName(), proto.getStartKey(),
        proto.getEndKey(), proto.getColumnFamily());
  }

  @Override
  public String toString() {
    return String.format("fileName: '%s', startKey: '%s', endKey: '%s'," +
        " columnFamily: '%s'", fileName, startKey, endKey, columnFamily);
  }

  /**
   * Builder of CompactionFileInfo.
   */
  public static class Builder {
    private final String fileName;
    private String startRange;
    private String endRange;
    private String columnFamily;

    public Builder(String fileName) {
      Preconditions.checkNotNull(fileName, "FileName is required parameter.");
      this.fileName = fileName;
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
          columnFamily);
    }
  }
}
