/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Metadata;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import static org.apache.ratis.thirdparty.io.grpc.Metadata.ASCII_STRING_MARSHALLER;

/**
 * Set of constants used in Ozone implementation.
 */
@InterfaceAudience.Private
public final class OzoneConsts {


  public static final String STORAGE_DIR = "scm";
  public static final String SCM_ID = "scmUuid";
  public static final String SCM_HA = "scmHA";
  public static final String CLUSTER_ID_PREFIX = "CID-";
  public static final String SCM_CERT_SERIAL_ID = "scmCertSerialId";
  public static final String PRIMARY_SCM_NODE_ID = "primaryScmNodeId";

  public static final String OZONE_SIMPLE_ROOT_USER = "root";
  public static final String OZONE_SIMPLE_HDFS_USER = "hdfs";

  public static final String STORAGE_ID = "storageID";
  public static final String DATANODE_UUID = "datanodeUuid";
  public static final String DATANODE_LAYOUT_VERSION_DIR = "dnlayoutversion";
  public static final String CLUSTER_ID = "clusterID";
  public static final String LAYOUTVERSION = "layOutVersion";
  public static final String CTIME = "ctime";
  /*
   * BucketName length is used for both buckets and volume lengths
   */
  public static final int OZONE_MIN_BUCKET_NAME_LENGTH = 3;
  public static final int OZONE_MAX_BUCKET_NAME_LENGTH = 63;

  public static final String OZONE_ACL_USER_TYPE = "user";
  public static final String OZONE_ACL_GROUP_TYPE = "group";
  public static final String OZONE_ACL_WORLD_TYPE = "world";
  public static final String OZONE_ACL_ANONYMOUS_TYPE = "anonymous";
  public static final String OZONE_ACL_IP_TYPE = "ip";

  public static final String OZONE_ACL_READ = "r";
  public static final String OZONE_ACL_WRITE = "w";
  public static final String OZONE_ACL_DELETE = "d";
  public static final String OZONE_ACL_LIST = "l";
  public static final String OZONE_ACL_ALL = "a";
  public static final String OZONE_ACL_NONE = "n";
  public static final String OZONE_ACL_CREATE = "c";
  public static final String OZONE_ACL_READ_ACL = "x";
  public static final String OZONE_ACL_WRITE_ACL = "y";


  public static final String OZONE_DATE_FORMAT =
      "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String OZONE_TIME_ZONE = "GMT";

  public static final String OZONE_COMPONENT = "component";
  public static final String OZONE_FUNCTION  = "function";
  public static final String OZONE_RESOURCE = "resource";
  public static final String OZONE_USER = "user";
  public static final String OZONE_REQUEST = "request";

  // OM Http server endpoints
  public static final String OZONE_OM_SERVICE_LIST_HTTP_ENDPOINT =
      "/serviceList";
  public static final String OZONE_DB_CHECKPOINT_HTTP_ENDPOINT =
      "/dbCheckpoint";

  // Ozone File System scheme
  public static final String OZONE_URI_SCHEME = "o3fs";
  public static final String OZONE_OFS_URI_SCHEME = "ofs";

  public static final String OZONE_RPC_SCHEME = "o3";
  public static final String OZONE_O3TRASH_URI_SCHEME = "o3trash";
  public static final String OZONE_HTTP_SCHEME = "http";
  public static final String OZONE_URI_DELIMITER = "/";
  public static final String OZONE_ROOT = OZONE_URI_DELIMITER;


  public static final String CONTAINER_EXTENSION = ".container";
  public static final String CONTAINER_META = ".meta";

  // Refer to {@link ContainerReader} for container storage layout on disk.
  public static final String CONTAINER_PREFIX  = "containers";
  public static final String CONTAINER_META_PATH = "metadata";
  public static final String CONTAINER_TEMPORARY_CHUNK_PREFIX = "tmp";
  public static final String CONTAINER_CHUNK_NAME_DELIMITER = ".";
  public static final String CONTAINER_ROOT_PREFIX = "repository";

  public static final String FILE_HASH = "SHA-256";
  public static final String MD5_HASH = "MD5";
  public static final String CHUNK_OVERWRITE = "OverWriteRequested";

  public static final int CHUNK_SIZE = 1 * 1024 * 1024; // 1 MB
  public static final long KB = 1024L;
  public static final long MB = KB * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;

  /**
   * level DB names used by SCM and data nodes.
   */
  public static final String CONTAINER_DB_SUFFIX = "container.db";
  public static final String PIPELINE_DB_SUFFIX = "pipeline.db";
  public static final String CRL_DB_SUFFIX = "crl.db";
  public static final String DN_CONTAINER_DB = "-dn-" + CONTAINER_DB_SUFFIX;
  public static final String DN_CRL_DB = "dn-" + CRL_DB_SUFFIX;
  public static final String CRL_DB_DIRECTORY_NAME = "crl";
  public static final String OM_DB_NAME = "om.db";
  public static final String SCM_DB_NAME = "scm.db";
  public static final String OM_DB_BACKUP_PREFIX = "om.db.backup.";
  public static final String SCM_DB_BACKUP_PREFIX = "scm.db.backup.";
  public static final String CONTAINER_DB_NAME = "container.db";

  public static final String STORAGE_DIR_CHUNKS = "chunks";
  public static final String OZONE_DB_CHECKPOINT_REQUEST_FLUSH =
      "flushBeforeCheckpoint";

  public static final String RANGER_OZONE_SERVICE_VERSION_KEY =
      "#RANGEROZONESERVICEVERSION";

  /**
   * Supports Bucket Versioning.
   */
  public enum Versioning {
    NOT_DEFINED, ENABLED, DISABLED;

    public static Versioning getVersioning(boolean versioning) {
      return versioning ? ENABLED : DISABLED;
    }
  }

  // Block ID prefixes used in datanode containers.
  public static final String DELETING_KEY_PREFIX = "#deleting#";

  // Metadata keys for datanode containers.
  public static final String DELETE_TRANSACTION_KEY = "#delTX";
  public static final String BLOCK_COMMIT_SEQUENCE_ID = "#BCSID";
  public static final String BLOCK_COUNT = "#BLOCKCOUNT";
  public static final String CONTAINER_BYTES_USED = "#BYTESUSED";
  public static final String PENDING_DELETE_BLOCK_COUNT =
      "#PENDINGDELETEBLOCKCOUNT";

  /**
   * OM LevelDB prefixes.
   *
   * OM DB stores metadata as KV pairs with certain prefixes,
   * prefix is used to improve the performance to get related
   * metadata.
   *
   * OM DB Schema:
   *  ----------------------------------------------------------
   *  |  KEY                                     |     VALUE   |
   *  ----------------------------------------------------------
   *  | $userName                                |  VolumeList |
   *  ----------------------------------------------------------
   *  | /#volumeName                             |  VolumeInfo |
   *  ----------------------------------------------------------
   *  | /#volumeName/#bucketName                 |  BucketInfo |
   *  ----------------------------------------------------------
   *  | /volumeName/bucketName/keyName           |  KeyInfo    |
   *  ----------------------------------------------------------
   *  | #deleting#/volumeName/bucketName/keyName |  KeyInfo    |
   *  ----------------------------------------------------------
   */

  public static final String OM_KEY_PREFIX = "/";
  public static final String OM_USER_PREFIX = "$";
  public static final String OM_S3_PREFIX = "S3:";
  public static final String OM_S3_VOLUME_PREFIX = "s3";
  public static final String OM_S3_SECRET = "S3Secret:";
  public static final String OM_PREFIX = "Prefix:";
  public static final String OM_TENANT = "Tenant:";

  /**
   *   Max chunk size limit.
   */
  public static final int OZONE_SCM_CHUNK_MAX_SIZE = 32 * 1024 * 1024;


  /**
   * Max OM Quota size of Long.MAX_VALUE.
   */
  public static final long MAX_QUOTA_IN_BYTES = Long.MAX_VALUE;

  /**
   * Quota RESET default is -1, which means quota is not set.
   */
  public static final long QUOTA_RESET = -1;
  public static final long OLD_QUOTA_DEFAULT = -2;

  /**
   * Quota Units.
   */
  public enum Units { TB, GB, MB, KB, B }

  /**
   * Max number of keys returned per list buckets operation.
   */
  public static final int MAX_LISTBUCKETS_SIZE  = 1024;

  /**
   * Max number of keys returned per list keys operation.
   */
  public static final int MAX_LISTKEYS_SIZE  = 1024;

  /**
   * Max number of volumes returned per list volumes operation.
   */
  public static final int MAX_LISTVOLUMES_SIZE = 1024;

  public static final int INVALID_PORT = -1;


  /**
   * Default SCM Datanode ID file name.
   */
  public static final String OZONE_SCM_DATANODE_ID_FILE_DEFAULT = "datanode.id";

  // The ServiceListJSONServlet context attribute where OzoneManager
  // instance gets stored.
  public static final String OM_CONTEXT_ATTRIBUTE = "ozone.om";

  public static final String SCM_CONTEXT_ATTRIBUTE = "ozone.scm";

  private OzoneConsts() {
    // Never Constructed
  }

  // YAML fields for .container files
  public static final String CONTAINER_ID = "containerID";
  public static final String CONTAINER_TYPE = "containerType";
  public static final String STATE = "state";
  public static final String METADATA = "metadata";
  public static final String MAX_SIZE = "maxSize";
  public static final String METADATA_PATH = "metadataPath";
  public static final String CHUNKS_PATH = "chunksPath";
  public static final String CONTAINER_DB_TYPE = "containerDBType";
  public static final String CHECKSUM = "checksum";
  public static final String DATA_SCAN_TIMESTAMP = "dataScanTimestamp";
  public static final String ORIGIN_PIPELINE_ID = "originPipelineId";
  public static final String ORIGIN_NODE_ID = "originNodeId";
  public static final String SCHEMA_VERSION = "schemaVersion";
  public static final String REPLICA_INDEX = "replicaIndex";

  // Supported .container datanode schema versions.
  // Since containers in older schema versions are currently not reformatted to
  // newer schema versions, a datanode may have containers with a mix of schema
  // versions, requiring this property to be tracked on a per container basis.
  // V1: All data in default column family.
  public static final String SCHEMA_V1 = "1";
  // V2: Metadata, block data, and delete transactions in their own
  // column families.
  public static final String SCHEMA_V2 = "2";
  // V3: Column families definitions are close to V2,
  // but have containerID as key prefixes.
  public static final String SCHEMA_V3 = "3";

  public static final String[] SCHEMA_VERSIONS =
      new String[] {SCHEMA_V1, SCHEMA_V2, SCHEMA_V3};

  // Supported store types.
  public static final String OZONE = "ozone";
  public static final String S3 = "s3";

  // For OM Audit usage
  public static final String VOLUME = "volume";
  public static final String BUCKET = "bucket";
  public static final String KEY = "key";
  public static final String SRC_KEY = "srcKey";
  public static final String DST_KEY = "dstKey";
  public static final String USED_BYTES = "usedBytes";
  public static final String USED_NAMESPACE = "usedNamespace";
  public static final String QUOTA_IN_BYTES = "quotaInBytes";
  public static final String QUOTA_IN_NAMESPACE = "quotaInNamespace";
  public static final String OBJECT_ID = "objectID";
  public static final String UPDATE_ID = "updateID";
  public static final String CLIENT_ID = "clientID";
  public static final String OWNER = "owner";
  public static final String ADMIN = "admin";
  public static final String USERNAME = "username";
  public static final String PREV_KEY = "prevKey";
  public static final String START_KEY = "startKey";
  public static final String MAX_KEYS = "maxKeys";
  public static final String PREFIX = "prefix";
  public static final String KEY_PREFIX = "keyPrefix";
  public static final String ACL = "acl";
  public static final String ACLS = "acls";
  public static final String USER_ACL = "userAcl";
  public static final String ADD_ACLS = "addAcls";
  public static final String REMOVE_ACLS = "removeAcls";
  public static final String MAX_NUM_OF_BUCKETS = "maxNumOfBuckets";
  public static final String TO_KEY_NAME = "toKeyName";
  public static final String STORAGE_TYPE = "storageType";
  public static final String RESOURCE_TYPE = "resourceType";
  public static final String IS_VERSION_ENABLED = "isVersionEnabled";
  public static final String CREATION_TIME = "creationTime";
  public static final String MODIFICATION_TIME = "modificationTime";
  public static final String DATA_SIZE = "dataSize";
  public static final String REPLICATION_TYPE = "replicationType";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String REPLICATION_CONFIG = "replicationConfig";
  public static final String KEY_LOCATION_INFO = "keyLocationInfo";
  public static final String MULTIPART_LIST = "multipartList";
  public static final String UPLOAD_ID = "uploadID";
  public static final String PART_NUMBER_MARKER = "partNumberMarker";
  public static final String MAX_PARTS = "maxParts";
  public static final String S3_BUCKET = "s3Bucket";
  public static final String S3_GETSECRET_USER = "S3GetSecretUser";
  public static final String S3_SETSECRET_USER = "S3SetSecretUser";
  public static final String S3_REVOKESECRET_USER = "S3RevokeSecretUser";
  public static final String RENAMED_KEYS_MAP = "renamedKeysMap";
  public static final String UNRENAMED_KEYS_MAP = "unRenamedKeysMap";
  public static final String MULTIPART_UPLOAD_PART_NUMBER = "partNumber";
  public static final String MULTIPART_UPLOAD_PART_NAME = "partName";
  public static final String BUCKET_ENCRYPTION_KEY = "bucketEncryptionKey";
  public static final String DELETED_KEYS_LIST = "deletedKeysList";
  public static final String UNDELETED_KEYS_LIST = "unDeletedKeysList";
  public static final String SOURCE_VOLUME = "sourceVolume";
  public static final String SOURCE_BUCKET = "sourceBucket";
  public static final String BUCKET_LAYOUT = "bucketLayout";
  public static final String TENANT = "tenant";
  public static final String USER_PREFIX = "userPrefix";

  // For multi-tenancy
  public static final String TENANT_ID_USERNAME_DELIMITER = "$";
  public static final String DEFAULT_TENANT_BUCKET_NAMESPACE_POLICY_SUFFIX =
      "-VolumeAccess";
  public static final String DEFAULT_TENANT_BUCKET_POLICY_SUFFIX =
      "-BucketAccess";
  // Predefined tenant roles
  // Tenant user role. All tenant users are assigned this role by default
  public static final String DEFAULT_TENANT_ROLE_USER_SUFFIX = "-UserRole";
  // Tenant admin role. All tenant admins are assigned this role
  public static final String DEFAULT_TENANT_ROLE_ADMIN_SUFFIX = "-AdminRole";

  // For OM metrics saving to a file
  public static final String OM_METRICS_FILE = "omMetrics";
  public static final String OM_METRICS_TEMP_FILE = OM_METRICS_FILE + ".tmp";

  // For Multipart upload
  public static final int OM_MULTIPART_MIN_SIZE = 5 * 1024 * 1024;

  // GRPC block token metadata header and context key
  public static final String OZONE_BLOCK_TOKEN = "blocktoken";
  public static final Context.Key<UserGroupInformation> UGI_CTX_KEY =
      Context.key("UGI");

  public static final Metadata.Key<String> OBT_METADATA_KEY =
      Metadata.Key.of(OZONE_BLOCK_TOKEN, ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> USER_METADATA_KEY =
      Metadata.Key.of(OZONE_USER, ASCII_STRING_MARSHALLER);

  public static final String RPC_PORT = "RPC";

  // Default OMServiceID for OM Ratis servers to use as RaftGroupId
  public static final String OM_SERVICE_ID_DEFAULT = "omServiceIdDefault";
  public static final String OM_DEFAULT_NODE_ID = "om1";

  public static final String JAVA_TMP_DIR = "java.io.tmpdir";
  public static final String LOCALHOST = "localhost";


  public static final int S3_BUCKET_MIN_LENGTH = 3;
  public static final int S3_BUCKET_MAX_LENGTH = 64;

  public static final int S3_SECRET_KEY_MIN_LENGTH = 8;

  public static final int S3_REQUEST_HEADER_METADATA_SIZE_LIMIT_KB = 2;

  //GDPR
  public static final String GDPR_FLAG = "gdprEnabled";
  public static final String GDPR_ALGORITHM_NAME = "AES";
  public static final int GDPR_DEFAULT_RANDOM_SECRET_LENGTH = 16;
  public static final Charset GDPR_CHARSET = StandardCharsets.UTF_8;
  public static final String GDPR_LENGTH = "length";
  public static final String GDPR_SECRET = "secret";
  public static final String GDPR_ALGORITHM = "algorithm";

  /**
   * Block key name as illegal characters
   *
   * This regular expression is used to check if key name
   * contains illegal characters when creating/renaming key.
   *
   * Avoid the following characters in a key name:
   * "\", "{", "}", "^", "<", ">", "#", "|", "%", "`", "[", "]", "~", "?"
   * and Non-printable ASCII characters (128–255 decimal characters).
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
   */
  public static final Pattern KEYNAME_ILLEGAL_CHARACTER_CHECK_REGEX  =
          Pattern.compile("^[^^{}<>^?%~#`\\[\\]\\|\\\\(\\x80-\\xff)]+$");

  public static final String FS_FILE_COPYING_TEMP_SUFFIX = "._COPYING_";

  // Transaction Info
  public static final String TRANSACTION_INFO_KEY = "#TRANSACTIONINFO";
  public static final String TRANSACTION_INFO_SPLIT_KEY = "#";

  public static final String PREPARE_MARKER_KEY = "#PREPAREDINFO";

  public static final String CONTAINER_DB_TYPE_ROCKSDB = "RocksDB";

  // SCM HA
  public static final String SCM_SERVICE_ID_DEFAULT = "scmServiceIdDefault";

  // SCM Ratis snapshot file to store the last applied index
  public static final String SCM_RATIS_SNAPSHOT_INDEX = "scmRatisSnapshotIndex";

  public static final String SCM_RATIS_SNAPSHOT_TERM = "scmRatisSnapshotTerm";
  // An on-disk transient marker file used when replacing DB with checkpoint
  public static final String DB_TRANSIENT_MARKER = "dbInconsistentMarker";

  // An on-disk marker file used to indicate that the OM is in prepare and
  // should remain prepared even after a restart.
  public static final String PREPARE_MARKER = "prepareMarker";

  // TODO : rename this to OZONE_RATIS_SNAPSHOT_DIR and use it in both
  // SCM and OM
  public static final String OM_RATIS_SNAPSHOT_DIR = "snapshot";
  public static final String SCM_RATIS_SNAPSHOT_DIR = "snapshot";

  public static final long DEFAULT_OM_UPDATE_ID = -1L;

  // SCM default service Id and node Id in non-HA where config is not defined
  // in non-HA style.
  public static final String SCM_DUMMY_NODEID = "scmNodeId";
  public static final String SCM_DUMMY_SERVICE_ID = "scmServiceId";

  // CRL Sequence Id
  public static final String CRL_SEQUENCE_ID_KEY = "CRL_SEQUENCE_ID";

  public static final String SCM_CA_PATH = "ca";
  public static final String SCM_CA_CERT_STORAGE_DIR = "scm";
  public static final String SCM_SUB_CA_PATH = "sub-ca";

  public static final String SCM_ROOT_CA_COMPONENT_NAME =
      Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString();

  public static final String SCM_SUB_CA_PREFIX = "scm-sub@";
  public static final String SCM_ROOT_CA_PREFIX = "scm@";

  // Layout Version written into Meta Table ONLY during finalization.
  public static final String LAYOUT_VERSION_KEY = "#LAYOUTVERSION";
  // Key written to Meta Table to indicate a component undergoing finalization.
  // Currently this is only used on SCM, but may be useful on OM if/when
  // finalizing one layout feature per Ratis request is implemented in
  // HDDS-4286.
  public static final String FINALIZING_KEY = "#FINALIZING";

  // Kerberos constants
  public static final String KERBEROS_CONFIG_VALUE = "kerberos";
  public static final String HTTP_AUTH_TYPE_SUFFIX = "http.auth.type";
  public static final String OZONE_SECURITY_ENABLED_SECURE = "true";
  public static final String OZONE_HTTP_SECURITY_ENABLED_SECURE = "true";
  public static final String OZONE_HTTP_FILTER_INITIALIZERS_SECURE =
      "org.apache.hadoop.security.AuthenticationFilterInitializer";

  public static final String DELEGATION_TOKEN_KIND = "kind";
  public static final String DELEGATION_TOKEN_SERVICE = "service";
  public static final String DELEGATION_TOKEN_RENEWER = "renewer";

  // EC Constants
  public static final String BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK = "blockGroupLen";

  public static final String OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT =
      "/service/xusers/secure/users";

  // Ideally we should use /addUsersAndGroups endpoint for add user to role,
  // but it always return 405 somehow.
  // https://ranger.apache.org/apidocs/resource_RoleREST.html
  // #resource_RoleREST_addUsersAndGroups_PUT
  public static final String OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT =
      "/service/roles/roles/";

  public static final String OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT =
      "/service/xusers/users/?name=";

  public static final String OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT =
      "/service/xusers/secure/users/id/";

  public static final String OZONE_OM_RANGER_ADMIN_CREATE_ROLE_HTTP_ENDPOINT =
      "/service/roles/roles";

  public static final String OZONE_OM_RANGER_ADMIN_GET_ROLE_HTTP_ENDPOINT =
      "/service/roles/roles/name/";

  public static final String OZONE_OM_RANGER_ADMIN_DELETE_GROUP_HTTP_ENDPOINT =
      "/service/xusers/secure/groups/id/";

  public static final String OZONE_OM_RANGER_ADMIN_DELETE_ROLE_HTTP_ENDPOINT =
      "/service/roles/roles/";

  public static final String OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT =
      "/service/public/v2/api/policy";

  public static final String OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT =
      "/service/public/v2/api/policy/?policyName=";

  public static final String OZONE_OM_RANGER_ADMIN_GET_POLICY_ID_HTTP_ENDPOINT =
      "/service/public/v2/api/policy/?policyId=";

  public static final String OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT =
      "/service/plugins/policies/";

  public static final String OZONE_OM_RANGER_OZONE_SERVICE_ENDPOINT =
      "/service/plugins/services/";

  public static final String OZONE_OM_RANGER_DOWNLOAD_ENDPOINT =
      "/service/plugins/secure/policies/download/cm_ozone" +
          "?supportsPolicyDeltas=true&lastKnownVersion=";

  public static final String OZONE_OM_RANGER_ALL_POLICIES_ENDPOINT =
      "/service/plugins/policies/service/";

  public static final String OZONE_TENANT_RANGER_POLICY_LABEL = "OzoneTenant";

  /**
   * The time (in ms) that AuthorizerLock try-lock operations would wait (by
   * default, some can be overridden) before declaring timeout.
   */
  public static final long OZONE_TENANT_AUTHORIZER_LOCK_WAIT_MILLIS = 1000L;

  /**
   * The maximum length of accessId allowed when assigning new users to a
   * tenant.
   */
  public static final int OZONE_MAXIMUM_ACCESS_ID_LENGTH = 100;
}
