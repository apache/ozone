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

package org.apache.ozone.fs.http;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Constants for the HttpFs server side implementations.
 */
public interface HttpFSConstants {

  String HTTP_GET = "GET";
  String HTTP_PUT = "PUT";
  String HTTP_POST = "POST";
  String HTTP_DELETE = "DELETE";

  String SCHEME = "webhdfs";
  String OP_PARAM = "op";
  String DO_AS_PARAM = "doas";
  String OVERWRITE_PARAM = "overwrite";
  String REPLICATION_PARAM = "replication";
  String BLOCKSIZE_PARAM = "blocksize";
  String PERMISSION_PARAM = "permission";
  String UNMASKED_PERMISSION_PARAM = "unmaskedpermission";
  String ACLSPEC_PARAM = "aclspec";
  String DESTINATION_PARAM = "destination";
  String RECURSIVE_PARAM = "recursive";
  String SOURCES_PARAM = "sources";
  String OWNER_PARAM = "owner";
  String GROUP_PARAM = "group";
  String MODIFICATION_TIME_PARAM = "modificationtime";
  String ACCESS_TIME_PARAM = "accesstime";
  String XATTR_NAME_PARAM = "xattr.name";
  String XATTR_VALUE_PARAM = "xattr.value";
  String XATTR_SET_FLAG_PARAM = "flag";
  String XATTR_ENCODING_PARAM = "encoding";
  String NEW_LENGTH_PARAM = "newlength";
  String START_AFTER_PARAM = "startAfter";
  String POLICY_NAME_PARAM = "storagepolicy";
  String SNAPSHOT_NAME_PARAM = "snapshotname";
  String OLD_SNAPSHOT_NAME_PARAM = "oldsnapshotname";
  String FSACTION_MODE_PARAM = "fsaction";
  String EC_POLICY_NAME_PARAM = "ecpolicy";
  Short DEFAULT_PERMISSION = 0755;
  String ACLSPEC_DEFAULT = "";
  String RENAME_JSON = "boolean";
  String TRUNCATE_JSON = "boolean";
  String DELETE_JSON = "boolean";
  String MKDIRS_JSON = "boolean";
  String HOME_DIR_JSON = "Path";
  String TRASH_DIR_JSON = "Path";
  String SET_REPLICATION_JSON = "boolean";
  String UPLOAD_CONTENT_TYPE = "application/octet-stream";
  String SNAPSHOT_JSON = "Path";
  String FILE_STATUSES_JSON = "FileStatuses";
  String FILE_STATUS_JSON = "FileStatus";
  String PATH_SUFFIX_JSON = "pathSuffix";
  String TYPE_JSON = "type";
  String LENGTH_JSON = "length";
  String OWNER_JSON = "owner";
  String GROUP_JSON = "group";
  String PERMISSION_JSON = "permission";
  String ACCESS_TIME_JSON = "accessTime";
  String MODIFICATION_TIME_JSON = "modificationTime";
  String BLOCK_SIZE_JSON = "blockSize";
  String CHILDREN_NUM_JSON = "childrenNum";
  String FILE_ID_JSON = "fileId";
  String REPLICATION_JSON = "replication";
  String STORAGEPOLICY_JSON = "storagePolicy";
  String ECPOLICYNAME_JSON = "ecPolicy";
  String XATTRS_JSON = "XAttrs";
  String XATTR_NAME_JSON = "name";
  String XATTR_VALUE_JSON = "value";
  String XATTRNAMES_JSON = "XAttrNames";
  String ECPOLICY_JSON = "ecPolicyObj";
  String SYMLINK_JSON = "symlink";
  String FILE_CHECKSUM_JSON = "FileChecksum";
  String CHECKSUM_ALGORITHM_JSON = "algorithm";
  String CHECKSUM_BYTES_JSON = "bytes";
  String CHECKSUM_LENGTH_JSON = "length";
  String CONTENT_SUMMARY_JSON = "ContentSummary";
  String CONTENT_SUMMARY_DIRECTORY_COUNT_JSON
      = "directoryCount";
  String CONTENT_SUMMARY_ECPOLICY_JSON = "ecPolicy";
  String CONTENT_SUMMARY_FILE_COUNT_JSON = "fileCount";
  String CONTENT_SUMMARY_LENGTH_JSON = "length";
  String QUOTA_USAGE_JSON = "QuotaUsage";
  String QUOTA_USAGE_FILE_AND_DIRECTORY_COUNT_JSON =
      "fileAndDirectoryCount";
  String QUOTA_USAGE_QUOTA_JSON = "quota";
  String QUOTA_USAGE_SPACE_CONSUMED_JSON = "spaceConsumed";
  String QUOTA_USAGE_SPACE_QUOTA_JSON = "spaceQuota";
  String QUOTA_USAGE_CONSUMED_JSON = "consumed";
  String QUOTA_USAGE_TYPE_QUOTA_JSON = "typeQuota";
  String ACL_STATUS_JSON = "AclStatus";
  String ACL_STICKY_BIT_JSON = "stickyBit";
  String ACL_ENTRIES_JSON = "entries";
  String ACL_BIT_JSON = "aclBit";
  String ENC_BIT_JSON = "encBit";
  String EC_BIT_JSON = "ecBit";
  String SNAPSHOT_BIT_JSON = "snapshotEnabled";
  String DIRECTORY_LISTING_JSON = "DirectoryListing";
  String PARTIAL_LISTING_JSON = "partialListing";
  String REMAINING_ENTRIES_JSON = "remainingEntries";
  String STORAGE_POLICIES_JSON = "BlockStoragePolicies";
  String STORAGE_POLICY_JSON = "BlockStoragePolicy";
  int HTTP_TEMPORARY_REDIRECT = 307;
  String SERVICE_NAME = "/webhdfs";
  String SERVICE_VERSION = "/v1";
  String SERVICE_PATH = SERVICE_NAME + SERVICE_VERSION;
  byte[] EMPTY_BYTES = {};

  /**
   * Converts a <code>FsPermission</code> to a Unix octal representation.
   *
   * @param p the permission.
   *
   * @return the Unix string symbolic reprentation.
   */
  static String permissionToString(FsPermission p) {
    return  Integer.toString((p == null) ? DEFAULT_PERMISSION : p.toShort(), 8);
  }

  /**
   * File types.
   */
  enum FILETYPE {
    FILE, DIRECTORY, SYMLINK;

    public static FILETYPE getType(FileStatus fileStatus) {
      if (fileStatus.isFile()) {
        return FILE;
      }
      if (fileStatus.isDirectory()) {
        return DIRECTORY;
      }
      if (fileStatus.isSymlink()) {
        return SYMLINK;
      }
      throw new IllegalArgumentException("Could not determine filetype for: " +
          fileStatus.getPath());
    }
  }

  /**
   * Operation types.
   */
  @InterfaceAudience.Private
  enum Operation {
    OPEN(HTTP_GET), GETFILESTATUS(HTTP_GET), LISTSTATUS(HTTP_GET),
    GETHOMEDIRECTORY(HTTP_GET), GETCONTENTSUMMARY(HTTP_GET),
    GETQUOTAUSAGE(HTTP_GET), GETFILECHECKSUM(HTTP_GET),
    GETFILEBLOCKLOCATIONS(HTTP_GET), INSTRUMENTATION(HTTP_GET),
    GETACLSTATUS(HTTP_GET), GETTRASHROOT(HTTP_GET),
    APPEND(HTTP_POST), CONCAT(HTTP_POST), TRUNCATE(HTTP_POST),
    CREATE(HTTP_PUT), MKDIRS(HTTP_PUT), RENAME(HTTP_PUT), SETOWNER(HTTP_PUT),
    SETPERMISSION(HTTP_PUT), SETREPLICATION(HTTP_PUT), SETTIMES(HTTP_PUT),
    MODIFYACLENTRIES(HTTP_PUT), REMOVEACLENTRIES(HTTP_PUT),
    REMOVEDEFAULTACL(HTTP_PUT), REMOVEACL(HTTP_PUT), SETACL(HTTP_PUT),
    DELETE(HTTP_DELETE), SETXATTR(HTTP_PUT), GETXATTRS(HTTP_GET),
    REMOVEXATTR(HTTP_PUT), LISTXATTRS(HTTP_GET), LISTSTATUS_BATCH(HTTP_GET),
    GETALLSTORAGEPOLICY(HTTP_GET), GETSTORAGEPOLICY(HTTP_GET),
    SETSTORAGEPOLICY(HTTP_PUT), UNSETSTORAGEPOLICY(HTTP_POST),
    ALLOWSNAPSHOT(HTTP_PUT), DISALLOWSNAPSHOT(HTTP_PUT),
    CREATESNAPSHOT(HTTP_PUT), DELETESNAPSHOT(HTTP_DELETE),
    RENAMESNAPSHOT(HTTP_PUT), GETSNAPSHOTDIFF(HTTP_GET),
    GETSNAPSHOTTABLEDIRECTORYLIST(HTTP_GET), GETSERVERDEFAULTS(HTTP_GET),
    CHECKACCESS(HTTP_GET), SETECPOLICY(HTTP_PUT), GETECPOLICY(HTTP_GET),
    UNSETECPOLICY(HTTP_POST), SATISFYSTORAGEPOLICY(HTTP_PUT);

    private String httpMethod;

    Operation(String httpMethod) {
      this.httpMethod = httpMethod;
    }

    public String getMethod() {
      return httpMethod;
    }

  }
}
