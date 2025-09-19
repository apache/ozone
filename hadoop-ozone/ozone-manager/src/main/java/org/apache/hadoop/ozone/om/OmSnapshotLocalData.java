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

import static java.util.Collections.unmodifiableList;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.ozone.compaction.log.SstFileInfo;
import org.rocksdb.LiveFileMetaData;
import org.yaml.snakeyaml.Yaml;

/**
 * OmSnapshotLocalData is the in-memory representation of snapshot local metadata.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerData
 */
public abstract class OmSnapshotLocalData {

  // Version of the snapshot local data. 0 indicates uncompacted snapshot.
  // compacted snapshots will have version > 0.
  private int version;

  // Checksum of the YAML representation
  private String checksum;

  // Whether SST is filtered
  private boolean isSSTFiltered;

  // Time of last compaction, in epoch milliseconds
  private long lastCompactionTime;

  // Whether the snapshot needs compaction
  private boolean needsCompaction;

  // Previous snapshotId based on which the snapshot local data is built.
  private UUID previousSnapshotId;

  // Map of version to VersionMeta, using linkedHashMap since the order of the map needs to be deterministic for
  // checksum computation.
  private final LinkedHashMap<Integer, VersionMeta> versionSstFileInfos;

  public static final Charset CHARSET_ENCODING = StandardCharsets.UTF_8;
  private static final String DUMMY_CHECKSUM = new String(new byte[64], CHARSET_ENCODING);

  /**
   * Creates a OmSnapshotLocalData object with default values.
   */
  public OmSnapshotLocalData(List<LiveFileMetaData> uncompactedSSTFileList, UUID previousSnapshotId) {
    this.isSSTFiltered = false;
    this.lastCompactionTime = 0L;
    this.needsCompaction = false;
    this.versionSstFileInfos = new LinkedHashMap<>();
    versionSstFileInfos.put(0,
        new VersionMeta(0, uncompactedSSTFileList.stream().map(SstFileInfo::new).collect(Collectors.toList())));
    this.version = 0;
    this.previousSnapshotId = previousSnapshotId;
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
    this.previousSnapshotId = source.previousSnapshotId;

    // Deep copy for compactedSSTFileList
    this.versionSstFileInfos = new LinkedHashMap<>();
    setVersionSstFileInfos(source.versionSstFileInfos);
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
  public Map<Integer, VersionMeta> getVersionSstFileInfos() {
    return Collections.unmodifiableMap(this.versionSstFileInfos);
  }

  /**
   * Sets the compacted SST file list.
   * @param versionSstFileInfos Map of version to compacted SST file list
   */
  public void setVersionSstFileInfos(Map<Integer, VersionMeta> versionSstFileInfos) {
    this.versionSstFileInfos.clear();
    this.versionSstFileInfos.putAll(versionSstFileInfos);
  }

  public UUID getPreviousSnapshotId() {
    return previousSnapshotId;
  }

  public void setPreviousSnapshotId(UUID previousSnapshotId) {
    this.previousSnapshotId = previousSnapshotId;
  }

  /**
   * Adds an entry to the compacted SST file list.
   * @param sstFiles SST file name
   */
  public void addVersionSSTFileInfos(List<SstFileInfo> sstFiles, int previousSnapshotVersion) {
    version++;
    this.versionSstFileInfos.put(version, new VersionMeta(previousSnapshotVersion, sstFiles));
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

  /**
   * Represents metadata for a specific version in a snapshot.
   * This class maintains the version of the previous snapshot and a list of SST (Sorted String Table) files
   * associated with the current version. It provides methods for accessing this data and supports a
   * copy mechanism for deep cloning.
   *
   * Instances of this class are immutable. The list of SST files is stored as an unmodifiable list to
   * maintain immutability.
   */
  public static class VersionMeta implements CopyObject<VersionMeta> {
    private final int previousSnapshotVersion;
    private final List<SstFileInfo> sstFiles;

    public VersionMeta(int previousSnapshotVersion, List<SstFileInfo> sstFiles) {
      this.previousSnapshotVersion = previousSnapshotVersion;
      this.sstFiles = unmodifiableList(sstFiles);
    }

    public int getPreviousSnapshotVersion() {
      return previousSnapshotVersion;
    }

    public List<SstFileInfo> getSstFiles() {
      return sstFiles;
    }

    @Override
    public VersionMeta copyObject() {
      return new VersionMeta(previousSnapshotVersion,
          sstFiles.stream().map(SstFileInfo::copyObject).collect(Collectors.toList()));
    }

    @Override
    public int hashCode() {
      return Objects.hash(previousSnapshotVersion, sstFiles);
    }

    @Override
    public final boolean equals(Object o) {
      if (!(o instanceof VersionMeta)) {
        return false;
      }
      VersionMeta that = (VersionMeta) o;
      return previousSnapshotVersion == that.previousSnapshotVersion && sstFiles.equals(that.sstFiles);
    }

    @Override
    public String toString() {
      return "VersionMeta{" +
          "previousSnapshotVersion=" + previousSnapshotVersion +
          ", sstFiles=" + sstFiles +
          '}';
    }
  }
}
