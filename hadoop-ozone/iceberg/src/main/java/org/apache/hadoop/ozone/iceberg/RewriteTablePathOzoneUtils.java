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

package org.apache.hadoop.ozone.iceberg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Pair;

final class RewriteTablePathOzoneUtils {

  private static final String RESULT_LOCATION = "file-list";
  
  private RewriteTablePathOzoneUtils() {
    // utility class
  }

  static Set<Pair<String, String>> statsFileCopyPlan(List<StatisticsFile> beforeStats,
                                                      List<StatisticsFile> afterStats) {
    Set<Pair<String, String>> result = new HashSet<>();
    if (beforeStats.isEmpty()) {
      return result;
    }

    if (beforeStats.size() != afterStats.size()) {
      throw new IllegalArgumentException("Before and after path rewrite, statistic files count should be same");
    }

    for (int i = 0; i < beforeStats.size(); i++) {
      StatisticsFile before = beforeStats.get(i);
      StatisticsFile after = afterStats.get(i);
      if (before.fileSizeInBytes() != after.fileSizeInBytes()) {
        throw new IllegalArgumentException("Before and after path rewrite, statistic files size should be same");
      }
      result.add(Pair.of(before.path(), after.path()));
    }
    return result;
  }

  static boolean fileExist(String path, FileIO io) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return io.newInputFile(path).exists();
  }

  static String getMetadataLocation(Table tbl) {
    String currentMetadataPath = ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(RewriteTablePathUtil.FILE_SEPARATOR);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    if (metadataDir.isEmpty()) {
      throw new IllegalArgumentException("Failed to get the metadata file root directory");
    }
    return metadataDir;
  }

  static void checkNonNullNonEmpty(String value, String name) {
    Objects.requireNonNull(value, () -> name + " is null");
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " is empty");
    }
  }

  static String saveFileList(Set<Pair<String, String>> filesToMove, String stagingDir, FileIO io) {
    String fileListPath = stagingDir + RESULT_LOCATION;
    OutputFile fileList = io.newOutputFile(fileListPath);
    writeAsCsv(filesToMove, fileList);
    return fileListPath;
  }

  static void writeAsCsv(Set<Pair<String, String>> rows, OutputFile outputFile) {
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(outputFile.createOrOverwrite(), StandardCharsets.UTF_8))) {
      for (Pair<String, String> pair : rows) {
        writer.append(pair.first()).append(',').append(pair.second());
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
}
