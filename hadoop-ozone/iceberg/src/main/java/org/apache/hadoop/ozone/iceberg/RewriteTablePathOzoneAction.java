/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.ozone.iceberg;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.base.Preconditions;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;

public class RewriteTablePathOzoneAction implements RewriteTablePath {

  private static final String RESULT_LOCATION = "file-list";

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;
  private int parallelism = Runtime.getRuntime().availableProcessors();

  private final Table table;
  private ExecutorService executorService;

  public RewriteTablePathOzoneAction(Table table) {
    this.table = table;
  }

  public RewriteTablePathOzoneAction(Table table, int parallelism) {
    this.table = table;
    this.parallelism = parallelism;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(
        sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    Preconditions.checkArgument(
        sVersion != null && !sVersion.trim().isEmpty(),
        "Start version('%s') cannot be empty.",
        sVersion);
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    Preconditions.checkArgument(
        eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.",
        eVersion);
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(
        stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.",
        stagingLocation);
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    this.executorService = Executors.newFixedThreadPool(parallelism);
    try {
      return doExecute();
    } finally {
      executorService.shutdown();
    }
  }

  private Result doExecute() {
    String resultLocation = rebuildMetadata();
    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(stagingDir)
        .fileListLocation(resultLocation)
        .latestVersion(RewriteTablePathUtil.fileName(endVersionName))
        .build();
  }

  private void validateInputs() {
    Preconditions.checkArgument(
        sourcePrefix != null && !sourcePrefix.isEmpty(),
        "Source prefix('%s') cannot be empty.",
        sourcePrefix);
    Preconditions.checkArgument(
        targetPrefix != null && !targetPrefix.isEmpty(),
        "Target prefix('%s') cannot be empty.",
        targetPrefix);
    Preconditions.checkArgument(
        !sourcePrefix.equals(targetPrefix),
        "Source prefix cannot be the same as target prefix (%s)",
        sourcePrefix);

    validateAndSetEndVersion();
    validateAndSetStartVersion();

    if (stagingDir == null) {
      stagingDir =
          getMetadataLocation(table)
              + "copy-table-staging-"
              + UUID.randomUUID()
              + RewriteTablePathUtil.FILE_SEPARATOR;
    } else {
      stagingDir = RewriteTablePathUtil.maybeAppendFileSeparator(stagingDir);
    }
  }

  private void validateAndSetEndVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (endVersionName == null) {
      Preconditions.checkNotNull(
          tableMetadata.metadataFileLocation(), "Metadata file location should not be null");
      this.endVersionName = tableMetadata.metadataFileLocation();
    } else {
      this.endVersionName = validateVersion(tableMetadata, endVersionName);
    }
  }

  private void validateAndSetStartVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (startVersionName != null) {
      this.startVersionName = validateVersion(tableMetadata, startVersionName);
    }
  }

  private String validateVersion(TableMetadata tableMetadata, String versionFileName) {
    String versionFile = null;
    if (versionInFilePath(tableMetadata.metadataFileLocation(), versionFileName)) {
      versionFile = tableMetadata.metadataFileLocation();
    }

    for (MetadataLogEntry log : tableMetadata.previousFiles()) {
      if (versionInFilePath(log.file(), versionFileName)) {
        versionFile = log.file();
      }
    }

    Preconditions.checkArgument(
        versionFile != null,
        "Cannot find provided version file %s in metadata log.",
        versionFileName);
    Preconditions.checkArgument(
        fileExist(versionFile), "Version file %s does not exist.", versionFile);
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String rebuildMetadata() {
    //TODO need to implement rewrite of metadata files , manifest list , manifest files and position delete files.
    return null;
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(RewriteTablePathUtil.FILE_SEPARATOR);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(
        !metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }
}
