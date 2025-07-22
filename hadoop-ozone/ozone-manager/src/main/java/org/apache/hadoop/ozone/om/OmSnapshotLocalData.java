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

package org.apache.hadoop.ozone.om;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * OmSnapshotLocalData is the in-memory representation of snapshot local metadata.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerData
 */
public abstract class OmSnapshotLocalData {

  // Version of the snapshot local data. A valid version shall be greater than 0.
  private int version;

  // Checksum of the YAML representation
  private String checksum;

  // Whether SST is filtered
  private boolean isSSTFiltered;

  // Map of Table to uncompacted SST file list on snapshot create
  private Map<String, List<String>> uncompactedSSTFileList;

  // Time of last compaction, in epoch milliseconds
  private long lastCompactionTime;

  // Whether the snapshot needs compaction
  private boolean needsCompaction;

  // Map of version to compacted SST file list
  // Map<version, Map<Table, sstFileList>>
  private Map<Integer, Map<String, List<String>>> compactedSSTFileList;

  public static final Charset CHARSET_ENCODING = StandardCharsets.UTF_8;
  private static final String DUMMY_CHECKSUM = new String(new byte[64], CHARSET_ENCODING);

  /**
   * Creates a OmSnapshotLocalData object with default values.
   */
  public OmSnapshotLocalData() {
    this.isSSTFiltered = false;
    this.uncompactedSSTFileList = new HashMap<>();
    this.lastCompactionTime = 0L;
    this.needsCompaction = false;
    this.compactedSSTFileList = new HashMap<>();
    this.version = 0;
    setChecksumTo0ByteArray();
  }

  /**
   * Copy constructor to create a deep copy of OmSnapshotLocalData object.
   * @param source The source OmSnapshotLocalData to copy from
   */
  public OmSnapshotLocalData(OmSnapshotLocalData source) {
    // Copy primitive fields directly
    this.isSSTFiltered = source.isSSTFiltered;
    this.lastCompactionTime = source.lastCompactionTime;
    this.needsCompaction = source.needsCompaction;
    this.checksum = source.checksum;
    this.version = source.version;

    // Deep copy for uncompactedSSTFileList
    this.uncompactedSSTFileList = new HashMap<>();
    for (Map.Entry<String, List<String>> entry :
        source.uncompactedSSTFileList.entrySet()) {
      this.uncompactedSSTFileList.put(
          entry.getKey(),
          Lists.newArrayList(entry.getValue()));
    }

    // Deep copy for compactedSSTFileList
    this.compactedSSTFileList = new HashMap<>();
    for (Map.Entry<Integer, Map<String, List<String>>> versionEntry :
        source.compactedSSTFileList.entrySet()) {
      Map<String, List<String>> tableMap = new HashMap<>();

      for (Map.Entry<String, List<String>> tableEntry :
          versionEntry.getValue().entrySet()) {
        tableMap.put(
            tableEntry.getKey(),
            Lists.newArrayList(tableEntry.getValue()));
      }

      this.compactedSSTFileList.put(versionEntry.getKey(), tableMap);
    }
  }

  /**
   * Returns whether SST is filtered for this snapshot.
   * @return true if SST is filtered, false otherwise
   */
  public boolean getSstFiltered() {
    return isSSTFiltered;
  }

  /**
   * Sets whether SST is filtered for this snapshot.
   * @param sstFiltered
   */
  public void setSstFiltered(boolean sstFiltered) {
    this.isSSTFiltered = sstFiltered;
  }

  /**
   * Returns the uncompacted SST file list.
   * @return Map of Table to uncompacted SST file list
   */
  public Map<String, List<String>> getUncompactedSSTFileList() {
    return Collections.unmodifiableMap(this.uncompactedSSTFileList);
  }

  /**
   * Sets the uncompacted SST file list.
   * @param uncompactedSSTFileList Map of Table to uncompacted SST file list
   */
  public void setUncompactedSSTFileList(
      Map<String, List<String>> uncompactedSSTFileList) {
    this.uncompactedSSTFileList.clear();
    this.uncompactedSSTFileList.putAll(uncompactedSSTFileList);
  }

  /**
   * Adds an entry to the uncompacted SST file list.
   * @param table Table name
   * @param sstFile SST file name
   */
  public void addUncompactedSSTFile(String table, String sstFile) {
    this.uncompactedSSTFileList.computeIfAbsent(table, k -> Lists.newArrayList())
        .add(sstFile);
  }

  /**
   * Returns the last compaction time, in epoch milliseconds.
   * @return Timestamp of the last compaction
   */
  public long getLastCompactionTime() {
    return lastCompactionTime;
  }

  /**
   * Sets the last compaction time, in epoch milliseconds.
   * @param lastCompactionTime Timestamp of the last compaction
   */
  public void setLastCompactionTime(Long lastCompactionTime) {
    this.lastCompactionTime = lastCompactionTime;
  }

  /**
   * Returns whether the snapshot needs compaction.
   * @return true if the snapshot needs compaction, false otherwise
   */
  public boolean getNeedsCompaction() {
    return needsCompaction;
  }

  /**
   * Sets whether the snapshot needs compaction.
   * @param needsCompaction true if the snapshot needs compaction, false otherwise
   */
  public void setNeedsCompaction(boolean needsCompaction) {
    this.needsCompaction = needsCompaction;
  }

  /**
   * Returns the compacted SST file list.
   * @return Map of version to compacted SST file list
   */
  public Map<Integer, Map<String, List<String>>> getCompactedSSTFileList() {
    return Collections.unmodifiableMap(this.compactedSSTFileList);
  }

  /**
   * Sets the compacted SST file list.
   * @param compactedSSTFileList Map of version to compacted SST file list
   */
  public void setCompactedSSTFileList(
      Map<Integer, Map<String, List<String>>> compactedSSTFileList) {
    this.compactedSSTFileList.clear();
    this.compactedSSTFileList.putAll(compactedSSTFileList);
  }

  /**
   * Adds an entry to the compacted SST file list.
   * @param ver Version number (TODO: to be clarified)
   * @param table Table name
   * @param sstFile SST file name
   */
  public void addCompactedSSTFile(Integer ver, String table, String sstFile) {
    this.compactedSSTFileList.computeIfAbsent(ver, k -> Maps.newHashMap())
        .computeIfAbsent(table, k -> Lists.newArrayList())
        .add(sstFile);
  }

  /**
   * Returns the checksum of the YAML representation.
   * @return checksum
   */
  public String getChecksum() {
    return checksum;
  }

  /**
   * Sets the checksum of the YAML representation.
   * @param checksum checksum
   */
  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  /**
   * Sets the checksum to a 0 byte array.
   */
  public void setChecksumTo0ByteArray() {
    this.checksum = DUMMY_CHECKSUM;
  }

  /**
   * Compute and set checksum for the snapshot data.
   * @param yaml Yaml instance for serialization
   * @throws IOException if checksum computation fails
   */
  public void computeAndSetChecksum(Yaml yaml) throws IOException {
    // Set checksum to dummy value - 0 byte array, to calculate the checksum
    // of rest of the data.
    setChecksumTo0ByteArray();

    // Dump yaml data into a string to compute its checksum
    String snapshotDataYamlStr = yaml.dump(this);

    this.checksum = getChecksum(snapshotDataYamlStr);
  }

  /**
   * Computes SHA-256 hash for a given string.
   * @param data String data for which checksum needs to be calculated
   * @return SHA-256 checksum as hex string
   * @throws IOException If checksum calculation fails
   */
  private static String getChecksum(String data) throws IOException {
    try {
      return DigestUtils.sha256Hex(data.getBytes(StandardCharsets.UTF_8));
    } catch (Exception ex) {
      throw new IOException("Unable to calculate checksum", ex);
    }
  }

  /**
   * Returns the version of the snapshot local data.
   * @return version
   */
  public int getVersion() {
    return version;
  }

  /**
   * Sets the version of the snapshot local data. A valid version shall be greater than 0.
   * @param version version
   */
  public void setVersion(int version) {
    this.version = version;
  }
}
