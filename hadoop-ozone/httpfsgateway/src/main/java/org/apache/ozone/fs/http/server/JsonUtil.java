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

package org.apache.ozone.fs.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

/** JSON Utilities. */
final class JsonUtil {
  // Reuse ObjectMapper instance for improving performance.
  // ObjectMapper is thread safe as long as we always configure instance
  // before use. We don't have a re-entrant call pattern in WebHDFS,
  // so we just need to worry about thread-safety.
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonUtil() {
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(key, value);
    try {
      return MAPPER.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  public static String toJsonString(Object obj) throws IOException {
    return MAPPER.writeValueAsString(obj);
  }

  public static String toJsonString(FsServerDefaults serverDefaults) {
    return toJsonString(FsServerDefaults.class, toJsonMap(serverDefaults));
  }

  private static Object toJsonMap(FsServerDefaults serverDefaults) {
    final Map<String, Object> m = new HashMap<String, Object>();
    m.put("blockSize", serverDefaults.getBlockSize());
    m.put("bytesPerChecksum", serverDefaults.getBytesPerChecksum());
    m.put("writePacketSize", serverDefaults.getWritePacketSize());
    m.put("replication", serverDefaults.getReplication());
    m.put("fileBufferSize", serverDefaults.getFileBufferSize());
    m.put("encryptDataTransfer", serverDefaults.getEncryptDataTransfer());
    m.put("trashInterval", serverDefaults.getTrashInterval());
    m.put("checksumType", serverDefaults.getChecksumType().id);
    m.put("keyProviderUri", serverDefaults.getKeyProviderUri());
    m.put("defaultStoragePolicyId", serverDefaults.getDefaultStoragePolicyId());
    return m;
  }

  public static String toJsonString(SnapshotDiffReport diffReport) {
    return toJsonString(SnapshotDiffReport.class.getSimpleName(),
        toJsonMap(diffReport));
  }

  private static Object toJsonMap(SnapshotDiffReport diffReport) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("snapshotRoot", diffReport.getSnapshotRoot());
    m.put("fromSnapshot", diffReport.getFromSnapshot());
    m.put("toSnapshot", diffReport.getLaterSnapshotName());
    Object[] diffList = new Object[diffReport.getDiffList().size()];
    for (int i = 0; i < diffReport.getDiffList().size(); i++) {
      diffList[i] = toJsonMap(diffReport.getDiffList().get(i));
    }
    m.put("diffList", diffList);
    return m;
  }

  private static Object toJsonMap(
      SnapshotDiffReport.DiffReportEntry diffReportEntry) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("type", diffReportEntry.getType());
    if (diffReportEntry.getSourcePath() != null) {
      m.put("sourcePath",
          DFSUtilClient.bytes2String(diffReportEntry.getSourcePath()));
    }
    if (diffReportEntry.getTargetPath() != null) {
      m.put("targetPath",
          DFSUtilClient.bytes2String(diffReportEntry.getTargetPath()));
    }
    return m;
  }

}
