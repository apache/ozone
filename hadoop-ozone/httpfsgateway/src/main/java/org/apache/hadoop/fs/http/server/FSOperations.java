/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem.FILE_TYPE;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.hadoop.fs.permission.FsCreateModes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.HTTPFS_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.HTTP_BUFFER_SIZE_DEFAULT;

/**
 * FileSystem operation executors used by {@link HttpFSServer}.
 */
@InterfaceAudience.Private
public final class FSOperations {

  private static int bufferSize = 4096;

  private FSOperations() {
    // not called
  }
  /**
   * Set the buffer size. The size is set during the initialization of
   * HttpFSServerWebApp.
   * @param conf the configuration to get the bufferSize
   */
  public static void setBufferSize(Configuration conf) {
    bufferSize = conf.getInt(HTTPFS_BUFFER_SIZE_KEY,
        HTTP_BUFFER_SIZE_DEFAULT);
  }

  /**
   * @param fileStatus a FileStatus object
   * @return JSON map suitable for wire transport
   */
  private static Map<String, Object> toJson(FileStatus fileStatus) {
    Map<String, Object> json = new LinkedHashMap<>();
    json.put(HttpFSFileSystem.FILE_STATUS_JSON, toJsonInner(fileStatus, true));
    return json;
  }

  /**
   * @param fileStatuses list of FileStatus objects
   * @param isFile is the fileStatuses from a file path
   * @return JSON map suitable for wire transport
   */
  @SuppressWarnings({"unchecked"})
  private static Map<String, Object> toJson(FileStatus[] fileStatuses,
      boolean isFile) {
    Map<String, Object> json = new LinkedHashMap<>();
    Map<String, Object> inner = new LinkedHashMap<>();
    JSONArray statuses = new JSONArray();
    for (FileStatus f : fileStatuses) {
      statuses.add(toJsonInner(f, isFile));
    }
    inner.put(HttpFSFileSystem.FILE_STATUS_JSON, statuses);
    json.put(HttpFSFileSystem.FILE_STATUSES_JSON, inner);
    return json;
  }

  /**
   * Not meant to be called directly except by the other toJson functions.
   */
  private static Map<String, Object> toJsonInner(FileStatus fileStatus,
      boolean emptyPathSuffix) {
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(HttpFSFileSystem.PATH_SUFFIX_JSON,
        (emptyPathSuffix) ? "" : fileStatus.getPath().getName());
    FILE_TYPE fileType = HttpFSFileSystem.FILE_TYPE.getType(fileStatus);
    json.put(HttpFSFileSystem.TYPE_JSON, fileType.toString());
    if (fileType.equals(FILE_TYPE.SYMLINK)) {
      // put the symlink into Json
      try {
        json.put(HttpFSFileSystem.SYMLINK_JSON,
            fileStatus.getSymlink().getName());
      } catch (IOException e) {
        // Can't happen.
      }
    }
    json.put(HttpFSFileSystem.LENGTH_JSON, fileStatus.getLen());
    json.put(HttpFSFileSystem.OWNER_JSON, fileStatus.getOwner());
    json.put(HttpFSFileSystem.GROUP_JSON, fileStatus.getGroup());
    json.put(HttpFSFileSystem.PERMISSION_JSON,
        HttpFSFileSystem.permissionToString(fileStatus.getPermission()));
    json.put(HttpFSFileSystem.ACCESS_TIME_JSON, fileStatus.getAccessTime());
    json.put(HttpFSFileSystem.MODIFICATION_TIME_JSON,
        fileStatus.getModificationTime());
    json.put(HttpFSFileSystem.BLOCK_SIZE_JSON, fileStatus.getBlockSize());
    json.put(HttpFSFileSystem.REPLICATION_JSON, fileStatus.getReplication());
    if (fileStatus instanceof HdfsFileStatus) {
      // Add HDFS-specific fields to response
      HdfsFileStatus hdfsFileStatus = (HdfsFileStatus) fileStatus;
      json.put(HttpFSFileSystem.CHILDREN_NUM_JSON,
          hdfsFileStatus.getChildrenNum());
      json.put(HttpFSFileSystem.FILE_ID_JSON,
          hdfsFileStatus.getFileId());
      json.put(HttpFSFileSystem.STORAGEPOLICY_JSON,
          hdfsFileStatus.getStoragePolicy());
      if (hdfsFileStatus.getErasureCodingPolicy() != null) {
        json.put(HttpFSFileSystem.ECPOLICYNAME_JSON,
            hdfsFileStatus.getErasureCodingPolicy().getName());
        json.put(HttpFSFileSystem.ECPOLICY_JSON,
            JsonUtil.getEcPolicyAsMap(
                hdfsFileStatus.getErasureCodingPolicy()));
      }
    }
    if (fileStatus.getPermission().getAclBit()) {
      json.put(HttpFSFileSystem.ACL_BIT_JSON, true);
    }
    if (fileStatus.getPermission().getEncryptedBit()) {
      json.put(HttpFSFileSystem.ENC_BIT_JSON, true);
    }
    if (fileStatus.getPermission().getErasureCodedBit()) {
      json.put(HttpFSFileSystem.EC_BIT_JSON, true);
    }
    if (fileStatus.isSnapshotEnabled()) {
      json.put(HttpFSFileSystem.SNAPSHOT_BIT_JSON, true);
    }
    return json;
  }

  /**
   * Serializes a DirectoryEntries object into the JSON for a
   * WebHDFS {@link org.apache.hadoop.hdfs.protocol.DirectoryListing}.
   * <p>
   * These two classes are slightly different, due to the impedance
   * mismatches between the WebHDFS and FileSystem APIs.
   * @param entries
   * @param isFile is the entries from a file path
   * @return json
   */
  private static Map<String, Object> toJson(FileSystem.DirectoryEntries
      entries, boolean isFile) {
    Map<String, Object> json = new LinkedHashMap<>();
    Map<String, Object> inner = new LinkedHashMap<>();
    Map<String, Object> fileStatuses = toJson(entries.getEntries(), isFile);
    inner.put(HttpFSFileSystem.PARTIAL_LISTING_JSON, fileStatuses);
    inner.put(HttpFSFileSystem.REMAINING_ENTRIES_JSON, entries.hasMore() ? 1
        : 0);
    json.put(HttpFSFileSystem.DIRECTORY_LISTING_JSON, inner);
    return json;
  }

  /** Converts an <code>AclStatus</code> object into a JSON object.
   *
   * @param aclStatus AclStatus object
   *
   * @return The JSON representation of the ACLs for the file
   */
  @SuppressWarnings({"unchecked"})
  private static Map<String,Object> aclStatusToJSON(AclStatus aclStatus) {
    Map<String,Object> json = new LinkedHashMap<String,Object>();
    Map<String,Object> inner = new LinkedHashMap<String,Object>();
    JSONArray entriesArray = new JSONArray();
    inner.put(HttpFSFileSystem.OWNER_JSON, aclStatus.getOwner());
    inner.put(HttpFSFileSystem.GROUP_JSON, aclStatus.getGroup());
    inner.put(HttpFSFileSystem.PERMISSION_JSON,
        HttpFSFileSystem.permissionToString(aclStatus.getPermission()));
    inner.put(HttpFSFileSystem.ACL_STICKY_BIT_JSON, aclStatus.isStickyBit());
    for ( AclEntry e : aclStatus.getEntries() ) {
      entriesArray.add(e.toString());
    }
    inner.put(HttpFSFileSystem.ACL_ENTRIES_JSON, entriesArray);
    json.put(HttpFSFileSystem.ACL_STATUS_JSON, inner);
    return json;
  }

  /**
   * Converts a <code>FileChecksum</code> object into a JSON array
   * object.
   *
   * @param checksum file checksum.
   *
   * @return The JSON representation of the file checksum.
   */
  @SuppressWarnings({"unchecked"})
  private static Map fileChecksumToJSON(FileChecksum checksum) {
    Map json = new LinkedHashMap();
    json.put(HttpFSFileSystem.CHECKSUM_ALGORITHM_JSON, checksum.getAlgorithmName());
    json.put(HttpFSFileSystem.CHECKSUM_BYTES_JSON,
             org.apache.hadoop.util.StringUtils.byteToHexString(checksum.getBytes()));
    json.put(HttpFSFileSystem.CHECKSUM_LENGTH_JSON, checksum.getLength());
    Map response = new LinkedHashMap();
    response.put(HttpFSFileSystem.FILE_CHECKSUM_JSON, json);
    return response;
  }

  /**
   * Converts xAttrs to a JSON object.
   *
   * @param xAttrs file xAttrs.
   * @param encoding format of xattr values.
   *
   * @return The JSON representation of the xAttrs.
   * @throws IOException 
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map xAttrsToJSON(Map<String, byte[]> xAttrs, 
      XAttrCodec encoding) throws IOException {
    Map jsonMap = new LinkedHashMap();
    JSONArray jsonArray = new JSONArray();
    if (xAttrs != null) {
      for (Entry<String, byte[]> e : xAttrs.entrySet()) {
        Map json = new LinkedHashMap();
        json.put(HttpFSFileSystem.XATTR_NAME_JSON, e.getKey());
        if (e.getValue() != null) {
          json.put(HttpFSFileSystem.XATTR_VALUE_JSON, 
              XAttrCodec.encodeValue(e.getValue(), encoding));
        }
        jsonArray.add(json);
      }
    }
    jsonMap.put(HttpFSFileSystem.XATTRS_JSON, jsonArray);
    return jsonMap;
  }

  /**
   * Converts xAttr names to a JSON object.
   *
   * @param names file xAttr names.
   *
   * @return The JSON representation of the xAttr names.
   * @throws IOException 
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map xAttrNamesToJSON(List<String> names) throws IOException {
    Map jsonMap = new LinkedHashMap();
    jsonMap.put(HttpFSFileSystem.XATTRNAMES_JSON, JSONArray.toJSONString(names));
    return jsonMap;
  }

  /**
   * Converts a <code>ContentSummary</code> object into a JSON array
   * object.
   *
   * @param contentSummary the content summary
   *
   * @return The JSON representation of the content summary.
   */
  @SuppressWarnings({"unchecked"})
  private static Map contentSummaryToJSON(ContentSummary contentSummary) {
    Map json = new LinkedHashMap();
    json.put(HttpFSFileSystem.CONTENT_SUMMARY_DIRECTORY_COUNT_JSON,
        contentSummary.getDirectoryCount());
    json.put(HttpFSFileSystem.CONTENT_SUMMARY_ECPOLICY_JSON,
        contentSummary.getErasureCodingPolicy());
    json.put(HttpFSFileSystem.CONTENT_SUMMARY_FILE_COUNT_JSON,
        contentSummary.getFileCount());
    json.put(HttpFSFileSystem.CONTENT_SUMMARY_LENGTH_JSON,
        contentSummary.getLength());
    Map<String, Object> quotaUsageMap = quotaUsageToMap(contentSummary);
    for (Map.Entry<String, Object> e : quotaUsageMap.entrySet()) {
      // For ContentSummary we don't need this since we already have
      // separate count for file and directory.
      if (!e.getKey().equals(
          HttpFSFileSystem.QUOTA_USAGE_FILE_AND_DIRECTORY_COUNT_JSON)) {
        json.put(e.getKey(), e.getValue());
      }
    }
    Map response = new LinkedHashMap();
    response.put(HttpFSFileSystem.CONTENT_SUMMARY_JSON, json);
    return response;
  }

  /**
   * Converts a <code>QuotaUsage</code> object into a JSON array
   * object.
   */
  @SuppressWarnings({"unchecked"})
  private static Map quotaUsageToJSON(QuotaUsage quotaUsage) {
    Map response = new LinkedHashMap();
    Map quotaUsageMap = quotaUsageToMap(quotaUsage);
    response.put(HttpFSFileSystem.QUOTA_USAGE_JSON, quotaUsageMap);
    return response;
  }

  private static Map<String, Object> quotaUsageToMap(QuotaUsage quotaUsage) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put(HttpFSFileSystem.QUOTA_USAGE_FILE_AND_DIRECTORY_COUNT_JSON,
        quotaUsage.getFileAndDirectoryCount());
    result.put(HttpFSFileSystem.QUOTA_USAGE_QUOTA_JSON, quotaUsage.getQuota());
    result.put(HttpFSFileSystem.QUOTA_USAGE_SPACE_CONSUMED_JSON,
        quotaUsage.getSpaceConsumed());
    result.put(HttpFSFileSystem.QUOTA_USAGE_SPACE_QUOTA_JSON,
        quotaUsage.getSpaceQuota());
    Map<String, Map<String, Long>> typeQuota = new TreeMap<>();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      long tQuota = quotaUsage.getTypeQuota(t);
      if (tQuota != HdfsConstants.QUOTA_RESET) {
        Map<String, Long> type = typeQuota.get(t.toString());
        if (type == null) {
          type = new TreeMap<>();
          typeQuota.put(t.toString(), type);
        }
        type.put(HttpFSFileSystem.QUOTA_USAGE_QUOTA_JSON,
            quotaUsage.getTypeQuota(t));
        type.put(HttpFSFileSystem.QUOTA_USAGE_CONSUMED_JSON,
            quotaUsage.getTypeConsumed(t));
      }
    }
    result.put(HttpFSFileSystem.QUOTA_USAGE_TYPE_QUOTA_JSON, typeQuota);
    return result;
  }

  /**
   * Converts an object into a Json Map with with one key-value entry.
   * <p/>
   * It assumes the given value is either a JSON primitive type or a
   * <code>JsonAware</code> instance.
   *
   * @param name name for the key of the entry.
   * @param value for the value of the entry.
   *
   * @return the JSON representation of the key-value pair.
   */
  @SuppressWarnings("unchecked")
  private static JSONObject toJSON(String name, Object value) {
    JSONObject json = new JSONObject();
    json.put(name, value);
    return json;
  }

  @SuppressWarnings({ "unchecked" })
  private static JSONObject storagePolicyToJSON(BlockStoragePolicySpi policy) {
    BlockStoragePolicy p = (BlockStoragePolicy) policy;
    JSONObject policyJson = new JSONObject();
    policyJson.put("id", p.getId());
    policyJson.put("name", p.getName());
    policyJson.put("storageTypes", toJsonArray(p.getStorageTypes()));
    policyJson.put("creationFallbacks", toJsonArray(p.getCreationFallbacks()));
    policyJson.put("replicationFallbacks",
        toJsonArray(p.getReplicationFallbacks()));
    policyJson.put("copyOnCreateFile", p.isCopyOnCreateFile());
    return policyJson;
  }

  @SuppressWarnings("unchecked")
  private static JSONArray toJsonArray(StorageType[] storageTypes) {
    JSONArray jsonArray = new JSONArray();
    for (StorageType type : storageTypes) {
      jsonArray.add(type.toString());
    }
    return jsonArray;
  }

  @SuppressWarnings("unchecked")
  private static JSONObject storagePoliciesToJSON(
      Collection<? extends BlockStoragePolicySpi> storagePolicies) {
    JSONObject json = new JSONObject();
    JSONArray jsonArray = new JSONArray();
    JSONObject policies = new JSONObject();
    if (storagePolicies != null) {
      for (BlockStoragePolicySpi policy : storagePolicies) {
        JSONObject policyMap = storagePolicyToJSON(policy);
        jsonArray.add(policyMap);
      }
    }
    policies.put(HttpFSFileSystem.STORAGE_POLICY_JSON, jsonArray);
    json.put(HttpFSFileSystem.STORAGE_POLICIES_JSON, policies);
    return json;
  }

  /**
   * Executor that performs an append FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSAppend implements FileSystemAccess.FileSystemExecutor<Void> {
    private InputStream is;
    private Path path;

    /**
     * Creates an Append executor.
     *
     * @param is input stream to append.
     * @param path path of the file to append.
     */
    public FSAppend(InputStream is, String path) {
      this.is = is;
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      OutputStream os = fs.append(path, bufferSize);
      long bytes = copyBytes(is, os);
      HttpFSServerWebApp.get().getMetrics().incrBytesWritten(bytes);
      return null;
    }

  }

  /**
   * Executor that performs a concat FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSConcat implements FileSystemAccess.FileSystemExecutor<Void> {
    private Path path;
    private Path[] sources;

    /**
     * Creates a Concat executor.
     *
     * @param path target path to concat to.
     * @param sources comma separated absolute paths to use as sources.
     */
    public FSConcat(String path, String[] sources) {
      this.sources = new Path[sources.length];

      for(int i = 0; i < sources.length; i++) {
        this.sources[i] = new Path(sources[i]);
      }

      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.concat(path, sources);
      return null;
    }

  }

  /**
   * Executor that performs a truncate FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSTruncate implements 
      FileSystemAccess.FileSystemExecutor<JSONObject> {
    private Path path;
    private long newLength;

    /**
     * Creates a Truncate executor.
     *
     * @param path target path to truncate to.
     * @param newLength The size the file is to be truncated to.
     */
    public FSTruncate(String path, long newLength) {
      this.path = new Path(path);
      this.newLength = newLength;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return <code>true</code> if the file has been truncated to the desired,
     *         <code>false</code> if a background process of adjusting the 
     *         length of the last block has been started, and clients should 
     *         wait for it to complete before proceeding with further file 
     *         updates.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      boolean result = fs.truncate(path, newLength);
      HttpFSServerWebApp.get().getMetrics().incrOpsTruncate();
      return toJSON(
          StringUtils.toLowerCase(HttpFSFileSystem.TRUNCATE_JSON), result);
    }

  }

  /**
   * Executor that performs a content-summary FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSContentSummary implements FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    /**
     * Creates a content-summary executor.
     *
     * @param path the path to retrieve the content-summary.
     */
    public FSContentSummary(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return a Map object (JSON friendly) with the content-summary.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      ContentSummary contentSummary = fs.getContentSummary(path);
      return contentSummaryToJSON(contentSummary);
    }

  }

  /**
   * Executor that performs a quota-usage FileSystemAccess files system
   * operation.
   */
  @InterfaceAudience.Private
  public static class FSQuotaUsage
      implements FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    public FSQuotaUsage(String path) {
      this.path = new Path(path);
    }

    @Override
    public Map execute(FileSystem fs) throws IOException {
      QuotaUsage quotaUsage = fs.getQuotaUsage(path);
      return quotaUsageToJSON(quotaUsage);
    }
  }

  /**
   * Executor that performs a create FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSCreate implements FileSystemAccess.FileSystemExecutor<Void> {
    private InputStream is;
    private Path path;
    private short permission;
    private short unmaskedPermission;
    private boolean override;
    private short replication;
    private long blockSize;

    /**
     * Creates a Create executor.
     *
     * @param is input stream to for the file to create.
     * @param path path of the file to create.
     * @param perm permission for the file.
     * @param override if the file should be overriden if it already exist.
     * @param repl the replication factor for the file.
     * @param blockSize the block size for the file.
     * @param unmaskedPerm unmasked permissions for the file
     */
    public FSCreate(InputStream is, String path, short perm, boolean override,
                    short repl, long blockSize, short unmaskedPerm) {
      this.is = is;
      this.path = new Path(path);
      this.permission = perm;
      this.unmaskedPermission = unmaskedPerm;
      this.override = override;
      this.replication = repl;
      this.blockSize = blockSize;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return The URI of the created file.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (replication == -1) {
        replication = fs.getDefaultReplication(path);
      }
      if (blockSize == -1) {
        blockSize = fs.getDefaultBlockSize(path);
      }
      FsPermission fsPermission = new FsPermission(permission);
      if (unmaskedPermission != -1) {
        fsPermission = FsCreateModes.create(fsPermission,
            new FsPermission(unmaskedPermission));
      }
      OutputStream os = fs.create(path, fsPermission, override, bufferSize, replication, blockSize, null);
      long bytes = copyBytes(is, os);
      HttpFSServerWebApp.get().getMetrics().incrBytesWritten(bytes);
      return null;
    }

  }

  /**
   * These copyBytes methods combines the two different flavors used originally.
   * One with length and another one with buffer size.
   * In this impl, buffer size is determined internally, which is a singleton
   * normally set during initialization.
   * @param in the inputStream
   * @param out the outputStream
   * @return the totalBytes
   * @throws IOException the exception to be thrown.
   */
  public static long copyBytes(InputStream in, OutputStream out)
      throws IOException {
    return copyBytes(in, out, Long.MAX_VALUE);
  }

  public static long copyBytes(InputStream in, OutputStream out, long count)
      throws IOException {
    long totalBytes = 0;

    // If bufferSize is not initialized use 4k. This will not happen
    // if all callers check and set it.
    byte[] buf = new byte[bufferSize];
    long bytesRemaining = count;
    int bytesRead;

    try {
      while (bytesRemaining > 0) {
        int bytesToRead = (int)
            (bytesRemaining < buf.length ? bytesRemaining : buf.length);

        bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1) {
          break;
        }

        out.write(buf, 0, bytesRead);
        bytesRemaining -= bytesRead;
        totalBytes += bytesRead;
      }
      return totalBytes;
    } finally {
      // Originally IOUtils.copyBytes() were called with close=true. So we are
      // implementing the same behavior here.
      try {
        in.close();
      } finally {
        out.close();
      }
    }
  }

  /**
   * Executor that performs a delete FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSDelete implements FileSystemAccess.FileSystemExecutor<JSONObject> {
    private Path path;
    private boolean recursive;

    /**
     * Creates a Delete executor.
     *
     * @param path path to delete.
     * @param recursive if the delete should be recursive or not.
     */
    public FSDelete(String path, boolean recursive) {
      this.path = new Path(path);
      this.recursive = recursive;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return <code>true</code> if the delete operation was successful,
     *         <code>false</code> otherwise.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      boolean deleted = fs.delete(path, recursive);
      HttpFSServerWebApp.get().getMetrics().incrOpsDelete();
      return toJSON(
          StringUtils.toLowerCase(HttpFSFileSystem.DELETE_JSON), deleted);
    }

  }

  /**
   * Executor that performs a file-checksum FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSFileChecksum implements FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    /**
     * Creates a file-checksum executor.
     *
     * @param path the path to retrieve the checksum.
     */
    public FSFileChecksum(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return a Map object (JSON friendly) with the file checksum.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      FileChecksum checksum = fs.getFileChecksum(path);
      return fileChecksumToJSON(checksum);
    }

  }

  /**
   * Executor that performs a file-status FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSFileStatus implements FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    /**
     * Creates a file-status executor.
     *
     * @param path the path to retrieve the status.
     */
    public FSFileStatus(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem getFileStatus operation and returns the
     * result in a JSONish Map.
     *
     * @param fs filesystem instance to use.
     *
     * @return a Map object (JSON friendly) with the file status.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      FileStatus status = fs.getFileStatus(path);
      HttpFSServerWebApp.get().getMetrics().incrOpsStat();
      return toJson(status);
    }

  }

  /**
   * Executor that performs a home-dir FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSHomeDir implements FileSystemAccess.FileSystemExecutor<JSONObject> {

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return a JSON object with the user home directory.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    @SuppressWarnings("unchecked")
    public JSONObject execute(FileSystem fs) throws IOException {
      Path homeDir = fs.getHomeDirectory();
      JSONObject json = new JSONObject();
      json.put(HttpFSFileSystem.HOME_DIR_JSON, homeDir.toUri().getPath());
      return json;
    }
  }

  /**
   * Executor that performs a list-status FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSListStatus implements FileSystemAccess.FileSystemExecutor<Map>, PathFilter {
    private Path path;
    private PathFilter filter;

    /**
     * Creates a list-status executor.
     *
     * @param path the directory/file to retrieve the status of its contents.
     * @param filter glob filter to use.
     *
     * @throws IOException thrown if the filter expression is incorrect.
     */
    public FSListStatus(String path, String filter) throws IOException {
      this.path = new Path(path);
      this.filter = (filter == null) ? this : new GlobFilter(filter);
    }

    /**
     * Returns data for a JSON Map containing the information for
     * the set of files in 'path' that match 'filter'.
     *
     * @param fs filesystem instance to use.
     *
     * @return a Map with the file status of the directory
     *         contents that match the filter
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      FileStatus[] fileStatuses = fs.listStatus(path, filter);
      HttpFSServerWebApp.get().getMetrics().incrOpsListing();
      return toJson(fileStatuses, fs.getFileStatus(path).isFile());
    }

    @Override
    public boolean accept(Path path) {
      return true;
    }

  }

  /**
   * Executor that performs a batched directory listing.
   */
  @InterfaceAudience.Private
  public static class FSListStatusBatch implements FileSystemAccess
      .FileSystemExecutor<Map> {
    private final Path path;
    private final byte[] token;

    public FSListStatusBatch(String path, byte[] token) throws IOException {
      this.path = new Path(path);
      this.token = token.clone();
    }

    /**
     * Simple wrapper filesystem that exposes the protected batched
     * listStatus API so we can use it.
     */
    private static class WrappedFileSystem extends FilterFileSystem {
      public WrappedFileSystem(FileSystem f) {
        super(f);
      }

      @Override
      public DirectoryEntries listStatusBatch(Path f, byte[] token) throws
          FileNotFoundException, IOException {
        return super.listStatusBatch(f, token);
      }
    }

    @Override
    public Map execute(FileSystem fs) throws IOException {
      WrappedFileSystem wrappedFS = new WrappedFileSystem(fs);
      FileSystem.DirectoryEntries entries =
          wrappedFS.listStatusBatch(path, token);
      return toJson(entries, wrappedFS.getFileStatus(path).isFile());
    }
  }

  /**
   * Executor that performs a mkdirs FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSMkdirs implements FileSystemAccess.FileSystemExecutor<JSONObject> {

    private Path path;
    private short permission;
    private short unmaskedPermission;

    /**
     * Creates a mkdirs executor.
     *
     * @param path directory path to create.
     * @param permission permission to use.
     * @param unmaskedPermission unmasked permissions for the directory
     */
    public FSMkdirs(String path, short permission,
        short unmaskedPermission) {
      this.path = new Path(path);
      this.permission = permission;
      this.unmaskedPermission = unmaskedPermission;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return <code>true</code> if the mkdirs operation was successful,
     *         <code>false</code> otherwise.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      FsPermission fsPermission = new FsPermission(permission);
      if (unmaskedPermission != -1) {
        fsPermission = FsCreateModes.create(fsPermission,
            new FsPermission(unmaskedPermission));
      }
      boolean mkdirs = fs.mkdirs(path, fsPermission);
      HttpFSServerWebApp.get().getMetrics().incrOpsMkdir();
      return toJSON(HttpFSFileSystem.MKDIRS_JSON, mkdirs);
    }

  }

  /**
   * Executor that performs a open FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSOpen implements FileSystemAccess.FileSystemExecutor<InputStream> {
    private Path path;

    /**
     * Creates a open executor.
     *
     * @param path file to open.
     */
    public FSOpen(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return The inputstream of the file.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public InputStream execute(FileSystem fs) throws IOException {
      // Only updating ops count. bytesRead is updated in InputStreamEntity
      HttpFSServerWebApp.get().getMetrics().incrOpsOpen();
      return fs.open(path, bufferSize);
    }

  }

  /**
   * Executor that performs a rename FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSRename implements FileSystemAccess.FileSystemExecutor<JSONObject> {
    private Path path;
    private Path toPath;

    /**
     * Creates a rename executor.
     *
     * @param path path to rename.
     * @param toPath new name.
     */
    public FSRename(String path, String toPath) {
      this.path = new Path(path);
      this.toPath = new Path(toPath);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return <code>true</code> if the rename operation was successful,
     *         <code>false</code> otherwise.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      boolean renamed = fs.rename(path, toPath);
      HttpFSServerWebApp.get().getMetrics().incrOpsRename();
      return toJSON(HttpFSFileSystem.RENAME_JSON, renamed);
    }

  }

  /**
   * Executor that performs a set-owner FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSSetOwner implements FileSystemAccess.FileSystemExecutor<Void> {
    private Path path;
    private String owner;
    private String group;

    /**
     * Creates a set-owner executor.
     *
     * @param path the path to set the owner.
     * @param owner owner to set.
     * @param group group to set.
     */
    public FSSetOwner(String path, String owner, String group) {
      this.path = new Path(path);
      this.owner = owner;
      this.group = group;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.setOwner(path, owner, group);
      return null;
    }

  }

  /**
   * Executor that performs a set-permission FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSSetPermission implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private short permission;

    /**
     * Creates a set-permission executor.
     *
     * @param path path to set the permission.
     * @param permission permission to set.
     */
    public FSSetPermission(String path, short permission) {
      this.path = new Path(path);
      this.permission = permission;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      FsPermission fsPermission = new FsPermission(permission);
      fs.setPermission(path, fsPermission);
      return null;
    }

  }

  /**
   * Executor that sets the acl for a file in a FileSystem
   */
  @InterfaceAudience.Private
  public static class FSSetAcl implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private List<AclEntry> aclEntries;

    /**
     * Creates a set-acl executor.
     *
     * @param path path to set the acl.
     * @param aclSpec acl to set.
     */
    public FSSetAcl(String path, String aclSpec) {
      this.path = new Path(path);
      this.aclEntries = AclEntry.parseAclSpec(aclSpec, true);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.setAcl(path, aclEntries);
      return null;
    }

  }

  /**
   * Executor that removes all acls from a file in a FileSystem
   */
  @InterfaceAudience.Private
  public static class FSRemoveAcl implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    /**
     * Creates a remove-acl executor.
     *
     * @param path path from which to remove the acl.
     */
    public FSRemoveAcl(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.removeAcl(path);
      return null;
    }

  }

  /**
   * Executor that modifies acl entries for a file in a FileSystem
   */
  @InterfaceAudience.Private
  public static class FSModifyAclEntries implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private List<AclEntry> aclEntries;

    /**
     * Creates a modify-acl executor.
     *
     * @param path path to set the acl.
     * @param aclSpec acl to set.
     */
    public FSModifyAclEntries(String path, String aclSpec) {
      this.path = new Path(path);
      this.aclEntries = AclEntry.parseAclSpec(aclSpec, true);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.modifyAclEntries(path, aclEntries);
      return null;
    }

  }

  /**
   * Executor that removes acl entries from a file in a FileSystem
   */
  @InterfaceAudience.Private
  public static class FSRemoveAclEntries implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private List<AclEntry> aclEntries;

    /**
     * Creates a remove acl entry executor.
     *
     * @param path path to set the acl.
     * @param aclSpec acl parts to remove.
     */
    public FSRemoveAclEntries(String path, String aclSpec) {
      this.path = new Path(path);
      this.aclEntries = AclEntry.parseAclSpec(aclSpec, false);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.removeAclEntries(path, aclEntries);
      return null;
    }

  }

  /**
   * Executor that removes the default acl from a directory in a FileSystem
   */
  @InterfaceAudience.Private
  public static class FSRemoveDefaultAcl implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    /**
     * Creates an executor for removing the default acl.
     *
     * @param path path to set the acl.
     */
    public FSRemoveDefaultAcl(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.removeDefaultAcl(path);
      return null;
    }

  }

  /**
   * Executor that performs getting trash root FileSystemAccess
   * files system operation.
   */
  @InterfaceAudience.Private
  public static class FSTrashRoot
      implements FileSystemAccess.FileSystemExecutor<JSONObject> {
    private Path path;
    public FSTrashRoot(String path) {
      this.path = new Path(path);
    }

    @Override
    @SuppressWarnings("unchecked")
    public JSONObject execute(FileSystem fs) throws IOException {
      Path trashRoot = fs.getTrashRoot(this.path);
      JSONObject json = new JSONObject();
      json.put(HttpFSFileSystem.TRASH_DIR_JSON, trashRoot.toUri().getPath());
      return json;
    }

  }

  /**
   * Executor that gets the ACL information for a given file.
   */
  @InterfaceAudience.Private
  public static class FSAclStatus implements FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    /**
     * Creates an executor for getting the ACLs for a file.
     *
     * @param path the path to retrieve the ACLs.
     */
    public FSAclStatus(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return a Map object (JSON friendly) with the file status.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      AclStatus status = fs.getAclStatus(path);
      return aclStatusToJSON(status);
    }

  }

  /**
   * Executor that performs a set-replication FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSSetReplication implements FileSystemAccess.FileSystemExecutor<JSONObject> {
    private Path path;
    private short replication;

    /**
     * Creates a set-replication executor.
     *
     * @param path path to set the replication factor.
     * @param replication replication factor to set.
     */
    public FSSetReplication(String path, short replication) {
      this.path = new Path(path);
      this.replication = replication;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return <code>true</code> if the replication value was set,
     *         <code>false</code> otherwise.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    @SuppressWarnings("unchecked")
    public JSONObject execute(FileSystem fs) throws IOException {
      boolean ret = fs.setReplication(path, replication);
      JSONObject json = new JSONObject();
      json.put(HttpFSFileSystem.SET_REPLICATION_JSON, ret);
      return json;
    }

  }

  /**
   * Executor that performs a set-times FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSSetTimes implements FileSystemAccess.FileSystemExecutor<Void> {
    private Path path;
    private long mTime;
    private long aTime;

    /**
     * Creates a set-times executor.
     *
     * @param path path to set the times.
     * @param mTime modified time to set.
     * @param aTime access time to set.
     */
    public FSSetTimes(String path, long mTime, long aTime) {
      this.path = new Path(path);
      this.mTime = mTime;
      this.aTime = aTime;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return void.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.setTimes(path, mTime, aTime);
      return null;
    }

  }

  /**
   * Executor that performs a setxattr FileSystemAccess files system operation.
   */
  @InterfaceAudience.Private
  public static class FSSetXAttr implements 
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private String name;
    private byte[] value;
    private EnumSet<XAttrSetFlag> flag;

    public FSSetXAttr(String path, String name, String encodedValue, 
        EnumSet<XAttrSetFlag> flag) throws IOException {
      this.path = new Path(path);
      this.name = name;
      this.value = XAttrCodec.decodeValue(encodedValue);
      this.flag = flag;
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.setXAttr(path, name, value, flag);
      return null;
    }
  }

  /**
   * Executor that performs a removexattr FileSystemAccess files system 
   * operation.
   */
  @InterfaceAudience.Private
  public static class FSRemoveXAttr implements 
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private String name;

    public FSRemoveXAttr(String path, String name) {
      this.path = new Path(path);
      this.name = name;
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.removeXAttr(path, name);
      return null;
    }
  }

  /**
   * Executor that performs listing xattrs FileSystemAccess files system 
   * operation.
   */
  @SuppressWarnings("rawtypes")
  @InterfaceAudience.Private
  public static class FSListXAttrs implements 
      FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;

    /**
     * Creates listing xattrs executor.
     *
     * @param path the path to retrieve the xattrs.
     */
    public FSListXAttrs(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return Map a map object (JSON friendly) with the xattr names.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      List<String> names = fs.listXAttrs(path);
      return xAttrNamesToJSON(names);
    }
  }

  /**
   * Executor that performs getting xattrs FileSystemAccess files system 
   * operation.
   */
  @SuppressWarnings("rawtypes")
  @InterfaceAudience.Private
  public static class FSGetXAttrs implements 
      FileSystemAccess.FileSystemExecutor<Map> {
    private Path path;
    private List<String> names;
    private XAttrCodec encoding;

    /**
     * Creates getting xattrs executor.
     *
     * @param path the path to retrieve the xattrs.
     */
    public FSGetXAttrs(String path, List<String> names, XAttrCodec encoding) {
      this.path = new Path(path);
      this.names = names;
      this.encoding = encoding;
    }

    /**
     * Executes the filesystem operation.
     *
     * @param fs filesystem instance to use.
     *
     * @return Map a map object (JSON friendly) with the xattrs.
     *
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Map execute(FileSystem fs) throws IOException {
      Map<String, byte[]> xattrs = null;
      if (names != null && !names.isEmpty()) {
        xattrs = fs.getXAttrs(path, names);
      } else {
        xattrs = fs.getXAttrs(path);
      }
      return xAttrsToJSON(xattrs, encoding);
    }
  }

  /**
   * Executor that performs a getAllStoragePolicies FileSystemAccess files
   * system operation.
   */
  @SuppressWarnings({ "unchecked" })
  @InterfaceAudience.Private
  public static class FSGetAllStoragePolicies implements
      FileSystemAccess.FileSystemExecutor<JSONObject> {

    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      Collection<? extends BlockStoragePolicySpi> storagePolicies = fs
          .getAllStoragePolicies();
      return storagePoliciesToJSON(storagePolicies);
    }
  }

  /**
   * Executor that performs a getStoragePolicy FileSystemAccess files system
   * operation.
   */
  @SuppressWarnings({ "unchecked" })
  @InterfaceAudience.Private
  public static class FSGetStoragePolicy implements
      FileSystemAccess.FileSystemExecutor<JSONObject> {

    private Path path;

    public FSGetStoragePolicy(String path) {
      this.path = new Path(path);
    }

    @Override
    public JSONObject execute(FileSystem fs) throws IOException {
      BlockStoragePolicySpi storagePolicy = fs.getStoragePolicy(path);
      JSONObject json = new JSONObject();
      json.put(HttpFSFileSystem.STORAGE_POLICY_JSON,
          storagePolicyToJSON(storagePolicy));
      return json;
    }
  }

  /**
   * Executor that performs a setStoragePolicy FileSystemAccess files system
   * operation.
   */
  @InterfaceAudience.Private
  public static class FSSetStoragePolicy implements
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private String policyName;

    public FSSetStoragePolicy(String path, String policyName) {
      this.path = new Path(path);
      this.policyName = policyName;
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.setStoragePolicy(path, policyName);
      return null;
    }
  }

  /**
   * Executor that performs a unsetStoragePolicy FileSystemAccess files system
   * operation.
   */
  @InterfaceAudience.Private
  public static class FSUnsetStoragePolicy implements
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    public FSUnsetStoragePolicy(String path) {
      this.path = new Path(path);
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.unsetStoragePolicy(path);
      return null;
    }
  }

  /**
   *  Executor that performs an allowSnapshot operation.
   */
  @InterfaceAudience.Private
  public static class FSAllowSnapshot implements
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    /**
     * Creates a allowSnapshot executor.
     * @param path directory path to allow snapshot.
     */
    public FSAllowSnapshot(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.allowSnapshot(path);
      } else {
        throw new UnsupportedOperationException("allowSnapshot is not "
            + "supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }

  /**
   *  Executor that performs an disallowSnapshot operation.
   */
  @InterfaceAudience.Private
  public static class FSDisallowSnapshot implements
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    /**
     * Creates a disallowSnapshot executor.
     * @param path directory path to allow snapshot.
     */
    public FSDisallowSnapshot(String path) {
      this.path = new Path(path);
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.disallowSnapshot(path);
      } else {
        throw new UnsupportedOperationException("disallowSnapshot is not "
            + "supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }

  /**
   *  Executor that performs a createSnapshot FileSystemAccess operation.
   */
  @InterfaceAudience.Private
  public static class FSCreateSnapshot implements
      FileSystemAccess.FileSystemExecutor<String> {

    private Path path;
    private String snapshotName;

    /**
     * Creates a createSnapshot executor.
     * @param path directory path to be snapshotted.
     * @param snapshotName the snapshot name.
     */
    public FSCreateSnapshot(String path, String snapshotName) {
      this.path = new Path(path);
      this.snapshotName = snapshotName;
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return <code>Path</code> the complete path for newly created snapshot
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public String execute(FileSystem fs) throws IOException {
      Path snapshotPath = fs.createSnapshot(path, snapshotName);
      JSONObject json = toJSON(HttpFSFileSystem.HOME_DIR_JSON,
          snapshotPath.toString());
      return json.toJSONString().replaceAll("\\\\", "");
    }
  }

  /**
   *  Executor that performs a deleteSnapshot FileSystemAccess operation.
   */
  @InterfaceAudience.Private
  public static class FSDeleteSnapshot implements
      FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private String snapshotName;

    /**
     * Creates a deleteSnapshot executor.
     * @param path path for the snapshot to be deleted.
     * @param snapshotName snapshot name.
     */
    public FSDeleteSnapshot(String path, String snapshotName) {
      this.path = new Path(path);
      this.snapshotName = snapshotName;
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return void
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.deleteSnapshot(path, snapshotName);
      return null;
    }
  }

  /**
   *  Executor that performs a renameSnapshot FileSystemAccess operation.
   */
  @InterfaceAudience.Private
  public static class FSRenameSnapshot implements
      FileSystemAccess.FileSystemExecutor<Void> {
    private Path path;
    private String oldSnapshotName;
    private String snapshotName;

    /**
     * Creates a renameSnapshot executor.
     * @param path directory path of the snapshot to be renamed.
     * @param oldSnapshotName current snapshot name.
     * @param snapshotName new snapshot name to be set.
     */
    public FSRenameSnapshot(String path, String oldSnapshotName,
                            String snapshotName) {
      this.path = new Path(path);
      this.oldSnapshotName = oldSnapshotName;
      this.snapshotName = snapshotName;
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return void
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      fs.renameSnapshot(path, oldSnapshotName, snapshotName);
      return null;
    }
  }

  /**
   *  Executor that performs a getSnapshotDiff operation.
   */
  @InterfaceAudience.Private
  public static class FSGetSnapshotDiff implements
      FileSystemAccess.FileSystemExecutor<String> {
    private Path path;
    private String oldSnapshotName;
    private String snapshotName;

    /**
     * Creates a getSnapshotDiff executor.
     * @param path directory path of the snapshots to be examined.
     * @param oldSnapshotName Older snapshot name.
     * @param snapshotName Newer snapshot name.
     */
    public FSGetSnapshotDiff(String path, String oldSnapshotName,
        String snapshotName) {
      this.path = new Path(path);
      this.oldSnapshotName = oldSnapshotName;
      this.snapshotName = snapshotName;
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return A serialized JSON string of snapshot diffs.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public String execute(FileSystem fs) throws IOException {
      SnapshotDiffReport sdr = null;
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        sdr = dfs.getSnapshotDiffReport(path, oldSnapshotName, snapshotName);
      } else {
        throw new UnsupportedOperationException("getSnapshotDiff is not "
            + "supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      if (sdr != null) {
        return JsonUtil.toJsonString(sdr);
      } else {
        return "";
      }
    }
  }

  /**
   *  Executor that performs a getSnapshottableDirListing operation.
   */
  @InterfaceAudience.Private
  public static class FSGetSnapshottableDirListing implements
      FileSystemAccess.FileSystemExecutor<String> {

    /**
     * Creates a getSnapshottableDirListing executor.
     */
    public FSGetSnapshottableDirListing() {
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return A JSON string of all snapshottable directories.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public String execute(FileSystem fs) throws IOException {
      SnapshottableDirectoryStatus[] sds = null;
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        sds = dfs.getSnapshottableDirListing();
      } else {
        throw new UnsupportedOperationException("getSnapshottableDirListing is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return JsonUtil.toJsonString(sds);
    }
  }

  /**
   * Executor that performs a getServerDefaults operation.
   */
  @InterfaceAudience.Private
  public static class FSGetServerDefaults
      implements FileSystemAccess.FileSystemExecutor<String> {

    /**
     * Creates a getServerDefaults executor.
     */
    public FSGetServerDefaults() {
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @return A JSON string.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public String execute(FileSystem fs) throws IOException {
      FsServerDefaults sds = null;
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        sds = dfs.getServerDefaults();
      } else {
        throw new UnsupportedOperationException("getServerDefaults is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return JsonUtil.toJsonString(sds);
    }
  }

  /**
   * Executor that performs a check access operation.
   */
  @InterfaceAudience.Private
  public static class FSAccess
      implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private FsAction mode;

    /**
     * Creates a access executor.
     */
    public FSAccess(String path, FsAction mode) {
      this.path = new Path(path);
      this.mode = mode;
    }

    /**
     * Executes the filesystem operation.
     * @param fs filesystem instance to use.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.access(path, mode);
        HttpFSServerWebApp.get().getMetrics().incrOpsCheckAccess();
      } else {
        throw new UnsupportedOperationException("checkaccess is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }

  /**
   * Executor that performs a setErasureCodingPolicy operation.
   */
  @InterfaceAudience.Private
  public static class FSSetErasureCodingPolicy
      implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;
    private String policyName;

    public FSSetErasureCodingPolicy(String path, String policyName) {
      this.path = new Path(path);
      this.policyName = policyName;
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.setErasureCodingPolicy(path, policyName);
      } else {
        throw new UnsupportedOperationException("setErasureCodingPolicy is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }

  /**
   * Executor that performs a getErasureCodingPolicy operation.
   */
  @InterfaceAudience.Private
  public static class FSGetErasureCodingPolicy
      implements FileSystemAccess.FileSystemExecutor<String> {

    private Path path;

    public FSGetErasureCodingPolicy(String path) {
      this.path = new Path(path);
    }

    @Override
    public String execute(FileSystem fs) throws IOException {
      ErasureCodingPolicy policy = null;
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        policy = dfs.getErasureCodingPolicy(path);
      } else {
        throw new UnsupportedOperationException("getErasureCodingPolicy is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return JsonUtil.toJsonString(policy);
    }
  }

  /**
   * Executor that performs a unsetErasureCodingPolicy operation.
   */
  @InterfaceAudience.Private
  public static class FSUnSetErasureCodingPolicy
      implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    public FSUnSetErasureCodingPolicy(String path) {
      this.path = new Path(path);
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.unsetErasureCodingPolicy(path);
      } else {
        throw new UnsupportedOperationException("unsetErasureCodingPolicy is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }

  /**
   * Executor that performs a satisfyStoragePolicy operation.
   */
  @InterfaceAudience.Private
  public static class FSSatisyStoragePolicy
      implements FileSystemAccess.FileSystemExecutor<Void> {

    private Path path;

    public FSSatisyStoragePolicy(String path) {
      this.path = new Path(path);
    }

    @Override
    public Void execute(FileSystem fs) throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.satisfyStoragePolicy(path);
      } else {
        throw new UnsupportedOperationException("satisfyStoragePolicy is "
            + "not supported for HttpFs on " + fs.getClass()
            + ". Please check your fs.defaultFS configuration");
      }
      return null;
    }
  }
}