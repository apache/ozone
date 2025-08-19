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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.codec.digest.DigestUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * OmSnapshotLocalData is the in-memory representation of snapshot local metadata.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerData
 */
public abstract class OmSnapshotLocalData {

  // Version of the snapshot local data. 0 indicates not defragged snapshot.
  // defragged snapshots will have version > 0.
  private int version;

  // Checksum of the YAML representation
  private String checksum;

  // Whether SST is filtered
  private boolean isSSTFiltered;

  // Map of Table to not defragged SST file list on snapshot create
  private Map<String, Set<String>> notDefraggedSSTFileList;

  // Time of last defragmentation, in epoch milliseconds
  private long lastDefragTime;

  // Whether the snapshot needs defragmentation
  private boolean needsDefragmentation;

  // Map of version to defragged SST file list
  // Map<version, Map<Table, sstFileList>>
  private Map<Integer, Map<String, Set<String>>> defraggedSSTFileList;

  public static final Charset CHARSET_ENCODING = StandardCharsets.UTF_8;
  private static final String DUMMY_CHECKSUM = new String(new byte[64], CHARSET_ENCODING);

  /**
   * Creates a OmSnapshotLocalData object with default values.
   */
  public OmSnapshotLocalData(Map<String, Set<String>> notDefraggedSSTFileList) {
    this.isSSTFiltered = false;
    this.notDefraggedSSTFileList = notDefraggedSSTFileList != null ? notDefraggedSSTFileList : new HashMap<>();
    this.lastDefragTime = 0L;
    this.needsDefragmentation = false;
    this.defraggedSSTFileList = new HashMap<>();
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
    this.lastDefragTime = source.lastDefragTime;
    this.needsDefragmentation = source.needsDefragmentation;
    this.checksum = source.checksum;
    this.version = source.version;

    // Deep copy for notDefraggedSSTFileList
    this.notDefraggedSSTFileList = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry :
        source.notDefraggedSSTFileList.entrySet()) {
      this.notDefraggedSSTFileList.put(
          entry.getKey(),
          new HashSet<>(entry.getValue()));
    }

    // Deep copy for defraggedSSTFileList
    this.defraggedSSTFileList = new HashMap<>();
    for (Map.Entry<Integer, Map<String, Set<String>>> versionEntry :
        source.defraggedSSTFileList.entrySet()) {
      Map<String, Set<String>> tableMap = new HashMap<>();

      for (Map.Entry<String, Set<String>> tableEntry :
          versionEntry.getValue().entrySet()) {
        tableMap.put(
            tableEntry.getKey(),
            new HashSet<>(tableEntry.getValue()));
      }

      this.defraggedSSTFileList.put(versionEntry.getKey(), tableMap);
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
   * Returns the not defragged SST file list.
   * @return Map of Table to not defragged SST file list
   */
  public Map<String, Set<String>> getNotDefraggedSSTFileList() {
    return Collections.unmodifiableMap(this.notDefraggedSSTFileList);
  }

  /**
   * Sets the not defragged SST file list.
   * @param notDefraggedSSTFileList Map of Table to not defragged SST file list
   */
  public void setNotDefraggedSSTFileList(
      Map<String, Set<String>> notDefraggedSSTFileList) {
    this.notDefraggedSSTFileList.clear();
    this.notDefraggedSSTFileList.putAll(notDefraggedSSTFileList);
  }

  /**
   * Adds an entry to the not defragged SST file list.
   * @param table Table name
   * @param sstFiles SST file name
   */
  public void addNotDefraggedSSTFileList(String table, Set<String> sstFiles) {
    this.notDefraggedSSTFileList.computeIfAbsent(table, k -> new HashSet<>())
        .addAll(sstFiles);
  }

  /**
   * Returns the last defragmentation time, in epoch milliseconds.
   * @return Timestamp of the last defragmentation
   */
  public long getLastDefragTime() {
    return lastDefragTime;
  }

  /**
   * Sets the last defragmentation time, in epoch milliseconds.
   * @param lastDefragTime Timestamp of the last defragmentation
   */
  public void setLastDefragTime(Long lastDefragTime) {
    this.lastDefragTime = lastDefragTime;
  }

  /**
   * Returns whether the snapshot needs defragmentation.
   * @return true if the snapshot needs defragmentation, false otherwise
   */
  public boolean getNeedsDefragmentation() {
    return needsDefragmentation;
  }

  /**
   * Sets whether the snapshot needs defragmentation.
   * @param needsDefragmentation true if the snapshot needs defragmentation, false otherwise
   */
  public void setNeedsDefragmentation(boolean needsDefragmentation) {
    this.needsDefragmentation = needsDefragmentation;
  }

  /**
   * Returns the defragged SST file list.
   * @return Map of version to defragged SST file list
   */
  public Map<Integer, Map<String, Set<String>>> getDefraggedSSTFileList() {
    return Collections.unmodifiableMap(this.defraggedSSTFileList);
  }

  /**
   * Sets the defragged SST file list.
   * @param defraggedSSTFileList Map of version to defragged SST file list
   */
  public void setDefraggedSSTFileList(
      Map<Integer, Map<String, Set<String>>> defraggedSSTFileList) {
    this.defraggedSSTFileList.clear();
    this.defraggedSSTFileList.putAll(defraggedSSTFileList);
  }

  /**
   * Adds an entry to the defragged SST file list.
   * @param ver Version number (TODO: to be clarified)
   * @param table Table name
   * @param sstFiles SST file name
   */
  public void addDefraggedSSTFileList(Integer ver, String table, Set<String> sstFiles) {
    this.defraggedSSTFileList.computeIfAbsent(ver, k -> Maps.newHashMap())
        .computeIfAbsent(table, k -> new HashSet<>())
        .addAll(sstFiles);
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
