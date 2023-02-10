/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.snapshot;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffReportProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DiffReportEntryProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DiffReportEntryProto.DiffTypeProto;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Snapshot diff report.
 */
public class SnapshotDiffReport {

  private static final String LINE_SEPARATOR = System.getProperty(
      "line.separator", "\n");

  /**
   * Types of the difference, which include CREATE, MODIFY, DELETE, and RENAME.
   * Each type has a label for representation:
   * +  CREATE
   * M  MODIFY
   * -  DELETE
   * R  RENAME
   */
  public enum DiffType {
    CREATE("+"),
    MODIFY("M"),
    DELETE("-"),
    RENAME("R");

    private final String label;

    DiffType(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    public DiffTypeProto toProtobuf() {
      return DiffTypeProto.valueOf(this.name());
    }

    public static DiffType fromProtobuf(final DiffTypeProto type) {
      return DiffType.valueOf(type.name());
    }
  }

  /**
   * Snapshot diff report entry.
   */
  public static final class DiffReportEntry {

    /**
     * The type of diff.
     */
    private final DiffType type;

    /**
     * Source File/Object path.
     */
    private final String sourcePath;

    /**
     * Destination File/Object path, if this is a re-name operation.
     */
    private final String targetPath;

    private DiffReportEntry(final DiffType type, final String sourcePath,
                            final String targetPath) {
      this.type = type;
      this.sourcePath = sourcePath;
      this.targetPath = targetPath;
    }

    public static DiffReportEntry of(final DiffType type,
                                     final String sourcePath) {
      return of(type, sourcePath, null);
    }

    public static DiffReportEntry of(final DiffType type,
                                     final String sourcePath,
                                     final String targetPath) {
      return new DiffReportEntry(type, sourcePath, targetPath);

    }

    @Override
    public String toString() {
      String str = type.getLabel() + "\t" + sourcePath;
      if (type == DiffType.RENAME) {
        str += " -> " + targetPath;
      }
      return str;
    }

    public DiffType getType() {
      return type;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof DiffReportEntry) {
        DiffReportEntry entry = (DiffReportEntry) other;
        return this.type.equals(entry.getType()) && this.sourcePath
            .equals(entry.sourcePath) && (this.targetPath != null ?
            this.targetPath.equals(entry.targetPath) : true);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    public DiffReportEntryProto toProtobuf() {
      final DiffReportEntryProto.Builder builder = DiffReportEntryProto
          .newBuilder();
      builder.setDiffType(type.toProtobuf()).setSourcePath(sourcePath);
      if (targetPath != null) {
        builder.setTargetPath(targetPath);
      }
      return builder.build();
    }

    public static DiffReportEntry fromProtobuf(
        final DiffReportEntryProto entry) {
      return of(DiffType.fromProtobuf(entry.getDiffType()),
          entry.getSourcePath(),
          entry.hasTargetPath() ? entry.getTargetPath() : null);
    }

  }


  /**
   * Volume name to which the snapshot bucket belongs.
   */
  private final String volumeName;

  /**
   * Bucket name to which the snapshot belongs.
   */
  private final String bucketName;
  /**
   * start point of the diff.
   */
  private final String fromSnapshot;

  /**
   * end point of the diff.
   */
  private final String toSnapshot;

  /**
   * list of diff.
   */
  private final List<DiffReportEntry> diffList;

  /**
   * subsequent token for the diff report.
   */
  private final String token = null; //TODO: will set it properly in HDDS-7548

  public SnapshotDiffReport(final String volumeName,
                            final String bucketName,
                            final String fromSnapshot,
                            final String toSnapshot,
                            final List<DiffReportEntry> entryList) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.diffList = entryList != null ? entryList : Collections.emptyList();
  }

  public List<DiffReportEntry> getDiffList() {
    return diffList;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("Difference between snapshot: ")
        .append(fromSnapshot)
        .append(" and snapshot: ")
        .append(toSnapshot)
        .append(",")
        .append(LINE_SEPARATOR);
    for (DiffReportEntry entry : diffList) {
      str.append(entry.toString()).append(LINE_SEPARATOR);
    }
    if (StringUtils.isNotEmpty(token)) {
      str.append("Next token: ")
          .append(token)
          .append(LINE_SEPARATOR);
    }
    return str.toString();
  }

  public SnapshotDiffReportProto toProtobuf() {
    final SnapshotDiffReportProto.Builder builder = SnapshotDiffReportProto
        .newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setFromSnapshot(fromSnapshot)
        .setToSnapshot(toSnapshot);
    builder.addAllDiffList(diffList.stream().map(DiffReportEntry::toProtobuf)
        .collect(Collectors.toList()));
    if (StringUtils.isNotEmpty(token)) {
      builder.setToken(token);
    }
    return builder.build();
  }

  public static SnapshotDiffReport fromProtobuf(
      final SnapshotDiffReportProto report) {
    return new SnapshotDiffReport(report.getVolumeName(),
        report.getBucketName(),
        report.getFromSnapshot(),
        report.getToSnapshot(),
        report.getDiffListList().stream()
            .map(DiffReportEntry::fromProtobuf)
            .collect(Collectors.toList()));
  }

}
