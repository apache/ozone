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

package org.apache.hadoop.ozone.om.snapshot;

import java.util.Collections;
import java.util.List;

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
        return type.equals(entry.getType())
            && sourcePath.equals(entry.sourcePath)
            && targetPath.equals(entry.targetPath);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
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

  public SnapshotDiffReport(final String volumeName, final String bucketName,
                            final String fromSnapshot, final String toSnapshot,
                            List<DiffReportEntry> entryList) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.diffList = entryList != null ? entryList : Collections.emptyList();
  }

  public List<DiffReportEntry> getDiffList() {
    return diffList;
  }
}
