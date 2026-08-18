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

package org.apache.hadoop.ozone.om.snapshot.diff;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;

/**
 * Metadata for a classified snapshot diff entry used to build the dependency
 * graph for dependency-ordered report emission.
 *
 * <p>Paths in the wrapped {@link DiffReportEntry} must use the snapshot
 * namespace expected for each diff type. {@link SnapDiffDependencyGraph} does not validate
 * namespaces; incorrect paths produce wrong edges rather than an error:
 * <ul>
 *   <li>DELETE and MODIFY: {@code sourcePath} is the from-snapshot path.</li>
 *   <li>CREATE: {@code sourcePath} is the to-snapshot path ({@code targetPath}
 *       is unset).</li>
 *   <li>RENAME: {@code sourcePath} is the from-snapshot path and
 *       {@code targetPath} is the to-snapshot path.</li>
 * </ul>
 *
 * <p>An entry carries both a source (from-snapshot) and a target (to-snapshot)
 * parent object id. For most diff types these are identical, but a RENAME can
 * move an object between parents, so the two ids may differ:
 * <ul>
 *   <li>The target parent drives to-snapshot hierarchy ordering (a parent must
 *       be created/renamed before its children).</li>
 *   <li>The source parent drives from-snapshot ordering (an entry must be
 *       applied before its source parent is deleted).</li>
 * </ul>
 */
public final class SnapDiffDependencyEntry {

  private final long objectId;
  private final long sourceParentObjectId;
  private final long targetParentObjectId;
  private final DiffReportEntry reportEntry;

  // Lazily decoded path caches. The graph reads each path several times while
  // building edges, so decoding once avoids repeated String allocation. These
  // are derived from reportEntry and are not part of equals/hashCode.
  private String sourcePath;
  private String targetPath;
  private boolean targetPathDecoded;

  /**
   * Creates an entry whose source and target parent are the same object.
   * Suitable for CREATE, DELETE and MODIFY where the object does not move.
   */
  public SnapDiffDependencyEntry(long objectId, long parentObjectId,
      DiffReportEntry reportEntry) {
    this(objectId, parentObjectId, parentObjectId, reportEntry);
  }

  /**
   * Creates an entry with distinct source and target parents. Use this for
   * RENAME entries that move an object between parents.
   */
  public SnapDiffDependencyEntry(long objectId, long sourceParentObjectId,
      long targetParentObjectId, DiffReportEntry reportEntry) {
    this.objectId = objectId;
    this.sourceParentObjectId = sourceParentObjectId;
    this.targetParentObjectId = targetParentObjectId;
    this.reportEntry = Objects.requireNonNull(reportEntry, "reportEntry");
  }

  public long getObjectId() {
    return objectId;
  }

  public long getSourceParentObjectId() {
    return sourceParentObjectId;
  }

  public long getTargetParentObjectId() {
    return targetParentObjectId;
  }

  public DiffReportEntry getReportEntry() {
    return reportEntry;
  }

  public DiffType getDiffType() {
    return reportEntry.getType();
  }

  public String getSourcePath() {
    String path = sourcePath;
    if (path == null) {
      path = new String(reportEntry.getSourcePath(), StandardCharsets.UTF_8);
      sourcePath = path;
    }
    return path;
  }

  public String getTargetPath() {
    if (!targetPathDecoded) {
      byte[] bytes = reportEntry.getTargetPath();
      targetPath = bytes == null
          ? null : new String(bytes, StandardCharsets.UTF_8);
      targetPathDecoded = true;
    }
    return targetPath;
  }

  public boolean isDelete() {
    return getDiffType() == DiffType.DELETE;
  }

  /**
   * Releases the cached decoded path strings. The graph reads paths repeatedly
   * while building edges, so decoding once is worthwhile during construction;
   * once the graph is built the caches are dead weight (~40-400 bytes/entry
   * depending on path length). Call after edge construction completes.
   *
   * <p>The underlying {@link DiffReportEntry} still holds the raw UTF-8 bytes,
   * so {@link #getSourcePath()} / {@link #getTargetPath()} will re-decode
   * on demand after this call.
   */
  void clearPathCache() {
    sourcePath = null;
    targetPath = null;
    targetPathDecoded = false;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SnapDiffDependencyEntry)) {
      return false;
    }
    SnapDiffDependencyEntry that = (SnapDiffDependencyEntry) other;
    return objectId == that.objectId
        && sourceParentObjectId == that.sourceParentObjectId
        && targetParentObjectId == that.targetParentObjectId
        && reportEntry.equals(that.reportEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectId, sourceParentObjectId, targetParentObjectId,
        reportEntry);
  }
}
