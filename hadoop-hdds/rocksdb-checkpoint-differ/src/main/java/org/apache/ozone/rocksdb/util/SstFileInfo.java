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

package org.apache.ozone.rocksdb.util;

import static org.apache.commons.io.FilenameUtils.getBaseName;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION;

import java.nio.file.Path;
import java.util.Objects;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.rocksdb.LiveFileMetaData;

/**
 * Dao to keep SST file information in the compaction log.
 */
public class SstFileInfo implements CopyObject<SstFileInfo> {
  private final String fileName;
  private final String startKey;
  private final String endKey;
  private final String columnFamily;

  public SstFileInfo(String fileName, String startRange, String endRange, String columnFamily) {
    this.fileName = fileName;
    this.startKey = startRange;
    this.endKey = endRange;
    this.columnFamily = columnFamily;
  }

  public SstFileInfo(LiveFileMetaData fileMetaData) {
    this(getBaseName(fileMetaData.fileName()), StringUtils.bytes2String(fileMetaData.smallestKey()),
        StringUtils.bytes2String(fileMetaData.largestKey()),
        StringUtils.bytes2String(fileMetaData.columnFamilyName()));
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

  @Override
  public String toString() {
    return String.format("fileName: '%s', startKey: '%s', endKey: '%s'," +
        " columnFamily: '%s'", fileName, startKey, endKey, columnFamily);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SstFileInfo)) {
      return false;
    }

    SstFileInfo that = (SstFileInfo) o;
    return Objects.equals(fileName, that.fileName) &&
        Objects.equals(startKey, that.startKey) &&
        Objects.equals(endKey, that.endKey) &&
        Objects.equals(columnFamily, that.columnFamily);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, startKey, endKey, columnFamily);
  }

  public Path getFilePath(Path directoryPath) {
    return directoryPath.resolve(getFileName() + SST_FILE_EXTENSION);
  }

  @Override
  public SstFileInfo copyObject() {
    return new SstFileInfo(fileName, startKey, endKey, columnFamily);
  }
}
