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

package org.apache.ozone.rocksdiff;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Dao to keep SST file information in the compaction log.
 */
public class CompactionFileInfo {
  private final String fileName;
  private final String startKey;
  private final String endKey;
  private final String columnFamily;

  public CompactionFileInfo(String fileName,
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
}
