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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.util.PersistentList;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Snapshot Diff Report.
 */
public class SnapshotDiffReport
    extends org.apache.hadoop.hdfs.protocol.SnapshotDiffReport {

  private static final String LINE_SEPARATOR =
      System.getProperty("line.separator", "\n");

  public SnapshotDiffReport(String snapshotRoot, String fromSnapshot,
      String toSnapshot, PersistentList<DiffReportEntry> entryList, String volumeName,
      String bucketName) {
    // TODO handle conversion from PersistentList to java.util.List
    super(snapshotRoot, fromSnapshot, toSnapshot, entryList);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }

  private String volumeName;

  private String bucketName;

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    String from = "snapshot " + getFromSnapshot();
    String to = "snapshot " + getToSnapshot();
    str.append("Difference between ").append(from).append(" and ").append(to)
        .append(":").append(LINE_SEPARATOR);
    for (DiffReportEntry entry : getDiffList()) {
      str.append(entry.toString()).append(LINE_SEPARATOR);
    }
    return str.toString();
  }

  public OzoneManagerProtocolProtos.SnapshotDiffReportProto toProtobuf() {
    final OzoneManagerProtocolProtos.SnapshotDiffReportProto.Builder builder =
        OzoneManagerProtocolProtos.SnapshotDiffReportProto.newBuilder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setFromSnapshot(getFromSnapshot()).setToSnapshot(getToSnapshot());
    builder.addAllDiffList(
        getDiffList().stream().map(x -> toProtobufDiffReportEntry(x))
            .collect(Collectors.toList()));
    return builder.build();
  }

  public static OzoneManagerProtocolProtos
      .DiffReportEntryProto toProtobufDiffReportEntry(DiffReportEntry entry) {
    final OzoneManagerProtocolProtos.DiffReportEntryProto.Builder builder =
        OzoneManagerProtocolProtos.DiffReportEntryProto.newBuilder();
    builder.setDiffType(toProtobufDiffType(entry.getType()))
        .setSourcePath(new String(entry.getSourcePath()));
    if (entry.getTargetPath() != null) {
      String targetPath = new String(entry.getTargetPath());
      builder.setTargetPath(targetPath);
    }
    return builder.build();
  }

  public static OzoneManagerProtocolProtos.DiffReportEntryProto
      .DiffTypeProto toProtobufDiffType(DiffType type) {
    return OzoneManagerProtocolProtos.DiffReportEntryProto
        .DiffTypeProto.valueOf(type.name());
  }

  public static SnapshotDiffReport fromProtobuf(
      final OzoneManagerProtocolProtos.SnapshotDiffReportProto report) {
    Path bucketPath = new Path(
        OZONE_URI_DELIMITER + report.getVolumeName()
            + OZONE_URI_DELIMITER + report.getBucketName());
    OFSPath path = new OFSPath(bucketPath, new OzoneConfiguration());

    // TODO handle conversion from PersistentList to java.util.List
    return new SnapshotDiffReport(path.toString(), report.getFromSnapshot(),
        report.getToSnapshot(), report.getDiffListList().stream()
        .map(SnapshotDiffReport::fromProtobufDiffReportEntry)
        .collect(Collectors.toList()), report.getVolumeName(),
        report.getBucketName());
  }

  public static DiffType fromProtobufDiffType(
      final OzoneManagerProtocolProtos.DiffReportEntryProto
          .DiffTypeProto type) {
    return DiffType.valueOf(type.name());
  }

  public static DiffReportEntry fromProtobufDiffReportEntry(
      final OzoneManagerProtocolProtos.DiffReportEntryProto entry) {
    if (entry == null) {
      return null;
    }
    DiffType type = fromProtobufDiffType(entry.getDiffType());
    return type == null ? null :
        new DiffReportEntry(type, entry.getSourcePath().getBytes(),
            entry.hasTargetPath() ? entry.getTargetPath().getBytes() : null);
  }
}
