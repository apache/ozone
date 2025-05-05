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

package org.apache.hadoop.ozone.snapshot;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DiffReportEntryProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffReportProto;

/**
 * Snapshot diff report.
 * <p>
 * This class is immutable.
 */
public class SnapshotDiffReportOzone
    extends org.apache.hadoop.hdfs.protocol.SnapshotDiffReport {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

  private static final Codec<DiffReportEntry> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(DiffReportEntryProto.getDefaultInstance()),
      SnapshotDiffReportOzone::fromProtobufDiffReportEntry,
      SnapshotDiffReportOzone::toProtobufDiffReportEntry,
      DiffReportEntry.class,
      DelegatedCodec.CopyType.SHALLOW);

  /**
   * Volume name to which the snapshot bucket belongs.
   */
  private final String volumeName;

  /**
   * Bucket name to which the snapshot belongs.
   */
  private final String bucketName;

  /**
   * subsequent token for the diff report.
   */
  private final String token;

  public SnapshotDiffReportOzone(final String snapshotRoot,
      final String volumeName,
      final String bucketName,
      final String fromSnapshot,
      final String toSnapshot,
      final List<DiffReportEntry> entryList, final String token) {
    super(snapshotRoot, fromSnapshot, toSnapshot, entryList);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.token = token;
  }

  public static Codec<DiffReportEntry> getDiffReportEntryCodec() {
    return CODEC;
  }

  @Override
  public List<DiffReportEntry> getDiffList() {
    return super.getDiffList();
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getToken() {
    return token;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("Difference between snapshot: ")
        .append(getFromSnapshot())
        .append(" and snapshot: ")
        .append(getLaterSnapshotName())
        .append(LINE_SEPARATOR);
    for (DiffReportEntry entry : getDiffList()) {
      str.append(entry.toString()).append(LINE_SEPARATOR);
    }
    if (StringUtils.isNotEmpty(token)) {
      str.append("Next token: ")
          .append(token);
    }
    return str.toString();
  }

  public SnapshotDiffReportProto toProtobuf() {
    final SnapshotDiffReportProto.Builder builder = SnapshotDiffReportProto
        .newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setFromSnapshot(getFromSnapshot())
        .setToSnapshot(getLaterSnapshotName());
    builder.addAllDiffList(getDiffList().stream().map(
            SnapshotDiffReportOzone::toProtobufDiffReportEntry)
        .collect(Collectors.toList()));
    if (StringUtils.isNotEmpty(token)) {
      builder.setToken(token);
    }
    return builder.build();
  }

  public static SnapshotDiffReportOzone fromProtobuf(
      final SnapshotDiffReportProto report) {
    Path bucketPath = new Path(
        OZONE_URI_DELIMITER + report.getVolumeName()
            + OZONE_URI_DELIMITER + report.getBucketName());
    OFSPath path = new OFSPath(bucketPath, new OzoneConfiguration());
    return new SnapshotDiffReportOzone(path.toString(),
        report.getVolumeName(),
        report.getBucketName(),
        report.getFromSnapshot(),
        report.getToSnapshot(),
        report.getDiffListList().stream()
            .map(SnapshotDiffReportOzone::fromProtobufDiffReportEntry)
            .collect(Collectors.toList()),
        report.hasToken() ? report.getToken() : null);
  }

  public static DiffType fromProtobufDiffType(
      final OzoneManagerProtocolProtos.DiffReportEntryProto
          .DiffTypeProto type) {
    return DiffType.valueOf(type.name());
  }

  public static OzoneManagerProtocolProtos.DiffReportEntryProto
      .DiffTypeProto toProtobufDiffType(DiffType type) {
    return OzoneManagerProtocolProtos.DiffReportEntryProto
        .DiffTypeProto.valueOf(type.name());
  }

  public static DiffReportEntry fromProtobufDiffReportEntry(
      final OzoneManagerProtocolProtos.DiffReportEntryProto entry) {
    if (entry == null) {
      return null;
    }
    DiffType type = fromProtobufDiffType(entry.getDiffType());
    return type == null ? null : new DiffReportEntry(type,
        entry.getSourcePath().getBytes(StandardCharsets.UTF_8),
        entry.hasTargetPath() ?
            entry.getTargetPath().getBytes(StandardCharsets.UTF_8) : null);
  }

  public static OzoneManagerProtocolProtos
      .DiffReportEntryProto toProtobufDiffReportEntry(DiffReportEntry entry) {
    final OzoneManagerProtocolProtos.DiffReportEntryProto.Builder builder =
        OzoneManagerProtocolProtos.DiffReportEntryProto.newBuilder();
    builder.setDiffType(toProtobufDiffType(entry.getType())).setSourcePath(
        new String(entry.getSourcePath(), StandardCharsets.UTF_8));
    if (entry.getTargetPath() != null) {
      String targetPath =
          new String(entry.getTargetPath(), StandardCharsets.UTF_8);
      builder.setTargetPath(targetPath);
    }
    return builder.build();
  }

  public static DiffReportEntry getDiffReportEntry(final DiffType type,
      final String sourcePath) {
    return getDiffReportEntry(type, sourcePath, null);
  }

  public static DiffReportEntry getDiffReportEntry(final DiffType type,
      final String sourcePath, final String targetPath) {
    return new DiffReportEntry(type,
        sourcePath.getBytes(StandardCharsets.UTF_8),
        targetPath != null ? targetPath.getBytes(StandardCharsets.UTF_8) :
            null);
  }

  /**
   * @param diffReport
   * Add the diffReportEntries from given diffReport to the report.
   * Used while aggregating paged response of snapdiff.
   */
  public void aggregate(SnapshotDiffReportOzone diffReport) {
    this.getDiffList().addAll(diffReport.getDiffList());
  }
}
