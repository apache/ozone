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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmInfo;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.OmUtils.MAX_TRXN_ID;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_TEMP_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.PREPARE_MARKER_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.RPC_PORT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NAMESPACE_STRICT_S3;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NAMESPACE_STRICT_S3_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_READ_THREADPOOL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_READ_THREADPOOL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GRPC_SERVER_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_PATH;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.getRaftGroupIdFromOmServiceId;
import static org.apache.hadoop.ozone.om.s3.S3SecretStoreConfigurationKeys.DEFAULT_SECRET_STORAGE_TYPE;
import static org.apache.hadoop.ozone.om.s3.S3SecretStoreConfigurationKeys.S3_SECRET_STORAGE_TYPE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.OzoneManagerInterService;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneManagerService;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.ozone.graph.PrintableGraph.GraphType.FILE_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ProtocolMessageEnum;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ReconfigureProtocolService;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolOmPB;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.OzoneSecurityException;
import org.apache.hadoop.hdds.security.symmetric.DefaultSecretKeyClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBUpdatesWrapper;
import org.apache.hadoop.hdds.utils.db.SequenceNumberNotFoundException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.execution.OMExecutionFlow;
import org.apache.hadoop.ozone.om.ha.OMHAMetrics;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocol.OMInterServiceProtocol;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolPB;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.ratis_snapshot.OmRatisSnapshotProvider;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.s3.S3SecretCacheProvider;
import org.apache.hadoop.ozone.om.s3.S3SecretStoreProvider;
import org.apache.hadoop.ozone.om.service.CompactDBService;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
import org.apache.hadoop.ozone.om.service.OMRangerBGSyncService;
import org.apache.hadoop.ozone.om.service.QuotaRepairTask;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMUpgradeFinalizer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OzoneManagerAdminService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedUserAccessIdInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantState;
import org.apache.hadoop.ozone.protocolPB.OMAdminProtocolServerSideImpl;
import org.apache.hadoop.ozone.protocolPB.OMInterServiceProtocolServerSideImpl;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.OMCertificateClient;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAuthorizerFactory;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.Time;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone Manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class OzoneManager extends ServiceRuntimeInfoImpl
    implements OzoneManagerProtocol, OMInterServiceProtocol, OMMXBean, Auditor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final AuditLogger SYSTEMAUDIT = new AuditLogger(
      AuditLoggerType.OMSYSTEMLOGGER);

  private static final String OM_DAEMON = "om";
  private static final String NO_LEADER_ERROR_MESSAGE =
          "There is no leader among the Ozone Manager servers. If this message " +
                  "persists, the service may be down. Possible cause: only one OM is up, or " +
                  "other OMs are unable to respond to Ratis leader vote messages";


  // This is set for read requests when OMRequest has S3Authentication set,
  // and it is reset when read request is processed.
  private static final ThreadLocal<S3Authentication> S3_AUTH =
      new ThreadLocal<>();

  private static boolean securityEnabled = false;

  private final ReconfigurationHandler reconfigurationHandler;
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private OzoneBlockTokenSecretManager blockTokenMgr;
  private CertificateClient certClient;
  private SecretKeyClient secretKeyClient;
  private ScmTopologyClient scmTopologyClient;
  private final Text omRpcAddressTxt;
  private OzoneConfiguration configuration;
  private OmConfig config;
  private RPC.Server omRpcServer;
  private GrpcOzoneManagerServer omS3gGrpcServer;
  private final InetSocketAddress omRpcAddress;
  private final String omId;
  private final String threadPrefix;
  private ServiceInfoProvider serviceInfo;

  private OMMetadataManager metadataManager;
  private OMMultiTenantManager multiTenantManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManagerImpl prefixManager;
  private final UpgradeFinalizer<OzoneManager> upgradeFinalizer;
  private ExecutorService edekCacheLoader = null;

  /**
   * OM super user / admin list.
   */
  private final String omStarterUser;
  private final OzoneAdmins omAdmins;
  private final OzoneAdmins readOnlyAdmins;
  private final OzoneAdmins s3OzoneAdmins;

  private final OMMetrics metrics;
  private OMHAMetrics omhaMetrics;
  private final ProtocolMessageMetrics<ProtocolMessageEnum>
      omClientProtocolMetrics;
  private final DeletingServiceMetrics omDeletionMetrics;
  private OzoneManagerHttpServer httpServer;
  private final OMStorage omStorage;
  private ObjectName omInfoBeanName;
  private Timer metricsTimer;
  private ScheduleOMMetricsWriteTask scheduleOMMetricsWriteTask;
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(OmMetricsInfo.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private final File omMetaDir;
  private boolean isAclEnabled;
  private final boolean isSpnegoEnabled;
  private final SecurityConfig secConfig;
  private S3SecretManager s3SecretManager;
  private final boolean isOmGrpcServerEnabled;
  private volatile boolean isOmRpcServerRunning = false;
  private volatile boolean isOmGrpcServerRunning = false;
  private String omComponent;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;

  private OzoneManagerRatisServer omRatisServer;
  private OMExecutionFlow omExecutionFlow;
  private OmRatisSnapshotProvider omRatisSnapshotProvider;
  private OMNodeDetails omNodeDetails;
  private final Map<String, OMNodeDetails> peerNodesMap;
  private File omRatisSnapshotDir;
  private final AtomicReference<TransactionInfo> omTransactionInfo
      = new AtomicReference<>(TransactionInfo.DEFAULT_VALUE);
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();
  private List<RatisDropwizardExports.MetricReporter> ratisReporterList = null;

  private KeyProviderCryptoExtension kmsProvider;
  private final OMLayoutVersionManager versionManager;

  private final ReplicationConfigValidator replicationConfigValidator;

  private boolean allowListAllVolumes;

  private int minMultipartUploadPartSize = OzoneConsts.OM_MULTIPART_MIN_SIZE;

  private final ScmClient scmClient;
  private final long scmBlockSize;
  private final int preallocateBlocksMax;
  private final boolean grpcBlockTokenEnabled;
  private final BucketLayout defaultBucketLayout;
  private ReplicationConfig defaultReplicationConfig;

  private final boolean isS3MultiTenancyEnabled;
  private final boolean isStrictS3;
  private ExitManager exitManager;

  private OzoneManagerPrepareState prepareState;

  private boolean isBootstrapping = false;
  private boolean isForcedBootstrapping = false;

  // Test flags
  private static boolean testReloadConfigFlag = false;
  private static boolean testSecureOmFlag = false;
  private static UserGroupInformation testUgi;

  private final OzoneLockProvider ozoneLockProvider;
  private final OMPerformanceMetrics perfMetrics;
  private final BucketUtilizationMetrics bucketUtilizationMetrics;

  private boolean fsSnapshotEnabled;

  private String omHostName;

  /**
   * OM Startup mode.
   */
  public enum StartupOption {
    REGUALR,
    BOOTSTRAP,
    FORCE_BOOTSTRAP
  }

  private enum State {
    INITIALIZED,
    BOOTSTRAPPING,
    RUNNING,
    STOPPED
  }

  // Used in MiniOzoneCluster testing
  private State omState;
  private Thread emptier;
  private OzoneTrash ozoneTrash;

  private static final int MSECS_PER_MINUTE = 60 * 1000;

  private final boolean isSecurityEnabled;

  private IAccessAuthorizer accessAuthorizer;
  // This metadata reader points to the active filesystem
  private OmMetadataReader omMetadataReader;
  // Wrap active DB metadata reader in ReferenceCounted once to avoid
  // instance creation every single time.
  private ReferenceCounted<IOmMetadataReader> rcOmMetadataReader;
  private OmSnapshotManager omSnapshotManager;

  @SuppressWarnings("methodlength")
  private OzoneManager(OzoneConfiguration conf, StartupOption startupOption)
      throws IOException, AuthenticationException {
    super(OzoneVersionInfo.OZONE_VERSION_INFO);
    Preconditions.checkNotNull(conf);
    setConfiguration(conf);
    // Load HA related configurations
    OMHANodeDetails omhaNodeDetails =
        OMHANodeDetails.loadOMHAConfig(configuration);

    this.isSecurityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    this.peerNodesMap = omhaNodeDetails.getPeerNodesMap();
    this.omNodeDetails = omhaNodeDetails.getLocalNodeDetails();
    omStorage = new OMStorage(conf);
    omStorage.validateOrPersistOmNodeId(omNodeDetails.getNodeId());
    omId = omStorage.getOmId();
    reconfigurationHandler =
        new ReconfigurationHandler("OM", conf, this::checkAdminUserPrivilege)
            .register(config)
            .register(OZONE_ADMINISTRATORS, this::reconfOzoneAdmins)
            .register(OZONE_READONLY_ADMINISTRATORS,
                this::reconfOzoneReadOnlyAdmins)
            .register(OZONE_OM_VOLUME_LISTALL_ALLOWED, this::reconfigureAllowListAllVolumes)
            .register(OZONE_KEY_DELETING_LIMIT_PER_TASK,
                this::reconfOzoneKeyDeletingLimitPerTask);

    versionManager = new OMLayoutVersionManager(omStorage.getLayoutVersion());
    upgradeFinalizer = new OMUpgradeFinalizer(versionManager);
    replicationConfigValidator =
        conf.getObject(ReplicationConfigValidator.class);

    exitManager = new ExitManager();

    // In case of single OM Node Service there will be no OM Node ID
    // specified, set it to value from om storage
    if (this.omNodeDetails.getNodeId() == null) {
      this.omNodeDetails = OMHANodeDetails.getOMNodeDetailsForNonHA(conf,
          omNodeDetails.getServiceId(),
          omStorage.getOmId(), omNodeDetails.getRpcAddress(),
          omNodeDetails.getRatisPort());
    }
    this.threadPrefix = omNodeDetails.threadNamePrefix();
    loginOMUserIfSecurityEnabled(conf);
    setInstanceVariablesFromConf();

    if (omStorage.getState() != StorageState.INITIALIZED) {
      throw new OMException("OM not initialized, current OM storage state: "
          + omStorage.getState().name() + ". Please ensure 'ozone om --init' "
          + "command is executed to generate all the required metadata to "
          + omStorage.getStorageDir()
          + " once before starting the OM service.",
          ResultCodes.OM_NOT_INITIALIZED);
    }
    omMetaDir = OMStorage.getOmDbDir(configuration);

    this.isSpnegoEnabled = conf.get(OZONE_OM_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");
    this.isOmGrpcServerEnabled = conf.getBoolean(
        OZONE_OM_S3_GPRC_SERVER_ENABLED,
        OZONE_OM_S3_GRPC_SERVER_ENABLED_DEFAULT);
    this.scmBlockSize = (long) conf.getStorageSize(OZONE_SCM_BLOCK_SIZE,
        OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    this.preallocateBlocksMax = conf.getInt(
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX,
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
    this.grpcBlockTokenEnabled = conf.getBoolean(HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.isStrictS3 = conf.getBoolean(
        OZONE_OM_NAMESPACE_STRICT_S3,
        OZONE_OM_NAMESPACE_STRICT_S3_DEFAULT);

    String defaultBucketLayoutString =
        configuration.getTrimmed(OZONE_DEFAULT_BUCKET_LAYOUT,
            OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT);

    boolean bucketLayoutValid = Arrays.stream(BucketLayout.values())
        .anyMatch(layout -> layout.name().equals(defaultBucketLayoutString));
    if (bucketLayoutValid) {
      this.defaultBucketLayout =
          BucketLayout.fromString(defaultBucketLayoutString);

      if (!defaultBucketLayout.isLegacy() &&
          !versionManager.isAllowed(OMLayoutFeature.BUCKET_LAYOUT_SUPPORT)) {
        LOG.warn("{} configured to non-legacy bucket layout {} when Ozone " +
            "Manager is pre-finalized for bucket layout support. Legacy " +
            "buckets will be created by default until Ozone Manager is " +
            "finalized.", OZONE_DEFAULT_BUCKET_LAYOUT, defaultBucketLayout);
      }
    } else {
      throw new ConfigurationException(defaultBucketLayoutString +
          " is not a valid default bucket layout. Supported values are " +
          Arrays.stream(BucketLayout.values())
              .map(Enum::toString).collect(Collectors.joining(", ")));
    }

    // Validates the default server-side replication configs.
    setReplicationFromConfig();
    InetSocketAddress omNodeRpcAddr = omNodeDetails.getRpcAddress();
    // Honor property 'hadoop.security.token.service.use_ip'
    omRpcAddressTxt = new Text(SecurityUtil.buildTokenService(omNodeRpcAddr));

    final StorageContainerLocationProtocol scmContainerClient = getScmContainerClient(configuration);
    // verifies that the SCM info in the OM Version file is correct.
    final ScmBlockLocationProtocol scmBlockClient = getScmBlockClient(configuration);
    scmTopologyClient = new ScmTopologyClient(scmBlockClient);
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient,
        configuration);
    this.ozoneLockProvider = new OzoneLockProvider(getKeyPathLockEnabled(),
        getEnableFileSystemPaths());

    // For testing purpose only, not hit scm from om as Hadoop UGI can't login
    // two principals in the same JVM.
    ScmInfo scmInfo;
    if (!testSecureOmFlag) {
      scmInfo = getScmInfo(configuration);
      if (!scmInfo.getClusterId().equals(omStorage.getClusterID())) {
        logVersionMismatch(conf, scmInfo);
        throw new OMException("SCM version info mismatch.",
            ResultCodes.SCM_VERSION_MISMATCH_ERROR);
      }
    } else {
      scmInfo = new ScmInfo.Builder().setScmId("test").build();
    }

    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    secConfig = new SecurityConfig(configuration);
    // Create the KMS Key Provider
    try {
      kmsProvider = createKeyProviderExt(configuration);
    } catch (IOException ioe) {
      kmsProvider = null;
      LOG.error("Fail to create Key Provider");
    }
    if (secConfig.isSecurityEnabled()) {
      omComponent = OM_DAEMON + "-" + omId;
      HddsProtos.OzoneManagerDetailsProto omInfo =
          getOmDetailsProto(conf, omStorage.getOmId());
      if (omStorage.getOmCertSerialId() == null) {
        throw new RuntimeException("OzoneManager started in secure mode but " +
            "doesn't have SCM signed certificate.");
      }
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
          getScmSecurityClientWithMaxRetry(configuration, getCurrentUser());
      certClient = new OMCertificateClient(secConfig, scmSecurityClient,
          omStorage, omInfo, "",
          scmInfo == null ? null : scmInfo.getScmId(),
          this::saveNewCertId, this::terminateOM);

      SecretKeyProtocol secretKeyProtocol =
          HddsServerUtil.getSecretKeyClientForOm(conf);
      secretKeyClient = DefaultSecretKeyClient.create(
          conf, secretKeyProtocol, omNodeDetails.threadNamePrefix());
    }
    serviceInfo = new ServiceInfoProvider(secConfig, this, certClient,
        testSecureOmFlag);

    if (secConfig.isBlockTokenEnabled()) {
      blockTokenMgr = createBlockTokenSecretManager();
    }

    // Enable S3 multi-tenancy if config keys are set
    this.isS3MultiTenancyEnabled =
        OMMultiTenantManager.checkAndEnableMultiTenancy(this, conf);

    metrics = OMMetrics.create();
    perfMetrics = OMPerformanceMetrics.register();
    omDeletionMetrics = DeletingServiceMetrics.create();
    // Get admin list
    omStarterUser = UserGroupInformation.getCurrentUser().getShortUserName();
    omAdmins = OzoneAdmins.getOzoneAdmins(omStarterUser, conf);
    LOG.info("OM start with adminUsers: {}", omAdmins.getAdminUsernames());

    // Get read only admin list
    readOnlyAdmins = OzoneAdmins.getReadonlyAdmins(conf);

    s3OzoneAdmins = OzoneAdmins.getS3Admins(conf);
    instantiateServices(false);

    // Create special volume s3v which is required for S3G.
    addS3GVolumeToDB();

    if (startupOption == StartupOption.BOOTSTRAP) {
      isBootstrapping = true;
    } else if (startupOption == StartupOption.FORCE_BOOTSTRAP) {
      isForcedBootstrapping = true;
    }

    initializeRatisDirs(conf);
    initializeRatisServer(isBootstrapping || isForcedBootstrapping);

    omClientProtocolMetrics = ProtocolMessageMetrics
        .create("OmClientProtocol", "Ozone Manager RPC endpoint",
            OzoneManagerProtocolProtos.Type.values());

    // Start Om Rpc Server.
    omRpcServer = getRpcServer(configuration);
    omRpcAddress = updateRPCListenAddress(configuration,
        OZONE_OM_ADDRESS_KEY, omNodeRpcAddr, omRpcServer);

    // Start S3g Om gRPC Server.
    if (isOmGrpcServerEnabled) {
      omS3gGrpcServer = getOmS3gGrpcServer(configuration);
    }

    // init om execution flow for request
    omExecutionFlow = new OMExecutionFlow(this);

    ShutdownHookManager.get().addShutdownHook(this::saveOmMetrics,
        SHUTDOWN_HOOK_PRIORITY);

    if (isBootstrapping || isForcedBootstrapping) {
      omState = State.BOOTSTRAPPING;
    } else {
      omState = State.INITIALIZED;
    }

    bucketUtilizationMetrics = BucketUtilizationMetrics.create(metadataManager);
    omHostName = HddsUtils.getHostName(conf);
  }

  public void initializeEdekCache(OzoneConfiguration conf) {
    int edekCacheLoaderDelay =
        conf.getInt(OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);
    int edekCacheLoaderInterval =
        conf.getInt(OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_KEY, OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_DEFAULT);
    int edekCacheLoaderMaxRetries =
        conf.getInt(OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_KEY, OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_DEFAULT);
    if (kmsProvider != null) {
      edekCacheLoader = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("Warm Up EDEK Cache Thread #%d")
              .build());
      warmUpEdekCache(edekCacheLoader, edekCacheLoaderDelay, edekCacheLoaderInterval, edekCacheLoaderMaxRetries);
    }
  }

  static class EDEKCacheLoader implements Runnable {
    private final String[] keyNames;
    private final KeyProviderCryptoExtension kp;
    private int initialDelay;
    private int retryInterval;
    private int maxRetries;

    EDEKCacheLoader(final String[] names, final KeyProviderCryptoExtension kp,
        final int delay, final int interval, final int maxRetries) {
      this.keyNames = names;
      this.kp = kp;
      this.initialDelay = delay;
      this.retryInterval = interval;
      this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
      LOG.info("Warming up {} EDEKs... (initialDelay={}, "
              + "retryInterval={}, maxRetries={})", keyNames.length, initialDelay, retryInterval,
          maxRetries);
      try {
        Thread.sleep(initialDelay);
      } catch (InterruptedException ie) {
        LOG.info("EDEKCacheLoader interrupted before warming up.");
        return;
      }

      boolean success = false;
      int retryCount = 0;
      IOException lastSeenIOE = null;
      long warmUpEDEKStartTime = monotonicNow();

      while (!success && retryCount < maxRetries) {
        try {
          kp.warmUpEncryptedKeys(keyNames);
          LOG.info("Successfully warmed up {} EDEKs.", keyNames.length);
          success = true;
        } catch (IOException ioe) {
          lastSeenIOE = ioe;
          LOG.info("Failed to warm up EDEKs.", ioe);
        } catch (Exception e) {
          LOG.error("Cannot warm up EDEKs.", e);
          throw e;
        }

        if (!success) {
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ie) {
            LOG.info("EDEKCacheLoader interrupted during retry.");
            break;
          }
          retryCount++;
        }
      }

      long warmUpEDEKTime = monotonicNow() - warmUpEDEKStartTime;
      LOG.debug("Time taken to load EDEK keys to the cache: {}", warmUpEDEKTime);
      if (!success) {
        LOG.warn("Max retry {} reached, unable to warm up EDEKs.", maxRetries);
        if (lastSeenIOE != null) {
          LOG.warn("Last seen exception:", lastSeenIOE);
        }
      }
    }
  }

  public void warmUpEdekCache(final ExecutorService executor, final int delay, final int interval, int maxRetries) {
    Set<String> keys = new HashSet<>();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>> iterator =
            metadataManager.getBucketTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> entry = iterator.next();
        if (entry.getValue().getEncryptionKeyInfo() != null) {
          String encKey = entry.getValue().getEncryptionKeyInfo().getKeyName();
          keys.add(encKey);
        }
      }
    } catch (IOException ex) {
      LOG.error("Error while retrieving encryption keys for warming up EDEK cache", ex);
    }
    String[] edeks = new String[keys.size()];
    edeks = keys.toArray(edeks);
    executor.execute(new EDEKCacheLoader(edeks, getKmsProvider(), delay, interval, maxRetries));
  }

  public boolean isStopped() {
    return omState == State.STOPPED;
  }

  /**
   * Set the {@link S3Authentication} for the current rpc handler thread.
   */
  public static void setS3Auth(S3Authentication val) {
    S3_AUTH.set(val);
  }

  /**
   * Returns the {@link S3Authentication} for the current rpc handler thread.
   */
  public static S3Authentication getS3Auth() {
    return S3_AUTH.get();
  }

  /** Returns the ThreadName prefix for the current OM. */
  public String getThreadNamePrefix() {
    return threadPrefix;
  }

  /**
   * This method is used to set selected instance variables in this class from
   * the passed in config. This allows these variable to be reset when the OM
   * instance is restarted (normally from a test mini-cluster). Note, not all
   * variables are added here as variables are selectively added as tests
   * require.
   */
  private void setInstanceVariablesFromConf() {
    this.isAclEnabled = configuration.getBoolean(OZONE_ACL_ENABLED,
        OZONE_ACL_ENABLED_DEFAULT);
    setAllowListAllVolumesFromConfig();
  }

  public void setAllowListAllVolumesFromConfig() {
    allowListAllVolumes = configuration.getBoolean(
        OZONE_OM_VOLUME_LISTALL_ALLOWED,
        OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT);
  }

  /**
   * Constructs OM instance based on the configuration.
   *
   * @param conf OzoneConfiguration
   * @return OM instance
   * @throws IOException AuthenticationException in case OM instance
   *                      creation fails,
   * @throws AuthenticationException
   */
  public static OzoneManager createOm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    return new OzoneManager(conf, StartupOption.REGUALR);
  }

  public static OzoneManager createOm(OzoneConfiguration conf,
      StartupOption startupOption) throws IOException, AuthenticationException {
    return new OzoneManager(conf, startupOption);
  }

  private void logVersionMismatch(OzoneConfiguration conf, ScmInfo scmInfo) {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
    StringBuilder scmBlockAddressBuilder = new StringBuilder();
    for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
      scmBlockAddressBuilder.append(scmNodeInfo.getBlockClientAddress())
          .append(",");
    }
    String scmBlockAddress = scmBlockAddressBuilder.toString();
    if (!StringUtils.isBlank(scmBlockAddress)) {
      scmBlockAddress = scmBlockAddress.substring(0,
          scmBlockAddress.lastIndexOf(","));
    }
    if (!scmInfo.getClusterId().equals(omStorage.getClusterID())) {
      LOG.error("clusterId from {} is {}, but is {} in {}",
          scmBlockAddress, scmInfo.getClusterId(),
          omStorage.getClusterID(), omStorage.getVersionFile());
    }
  }

  /**
   * Instantiate services which are dependent on the OM DB state.
   * When OM state is reloaded, these services are re-initialized with the
   * new OM state.
   */
  private void instantiateServices(boolean withNewSnapshot) throws IOException {

    OmMetadataManagerImpl metadataManagerImpl =
        new OmMetadataManagerImpl(configuration, this);
    this.metadataManager = metadataManagerImpl;
    LOG.info("S3 Multi-Tenancy is {}",
        isS3MultiTenancyEnabled ? "enabled" : "disabled");
    if (isS3MultiTenancyEnabled) {
      multiTenantManager = new OMMultiTenantManagerImpl(this, configuration);
      OzoneAclUtils.setOMMultiTenantManager(multiTenantManager);
    }

    volumeManager = new VolumeManagerImpl(metadataManager);

    bucketManager = new BucketManagerImpl(this, metadataManager);

    Class<? extends S3SecretStoreProvider> storeProviderClass =
        configuration.getClass(
            S3_SECRET_STORAGE_TYPE,
            DEFAULT_SECRET_STORAGE_TYPE,
            S3SecretStoreProvider.class);
    S3SecretStore store;
    try {
      store = storeProviderClass == DEFAULT_SECRET_STORAGE_TYPE
              ? metadataManagerImpl
              : storeProviderClass
                  .getConstructor().newInstance().get(configuration);
    } catch (Exception e) {
      throw new IOException(e);
    }
    S3SecretCacheProvider secretCacheProvider = S3SecretCacheProvider.IN_MEMORY;

    s3SecretManager = new S3SecretLockedManager(
        new S3SecretManagerImpl(
            store,
            secretCacheProvider.get(configuration)
        ),
        metadataManager.getLock()
    );
    if (secConfig.isSecurityEnabled() || testSecureOmFlag) {
      delegationTokenMgr = createDelegationTokenSecretManager(configuration);
    }

    prefixManager = new PrefixManagerImpl(this, metadataManager, true);
    keyManager = new KeyManagerImpl(this, scmClient, configuration,
        perfMetrics);
    // If authorizer is not initialized or the authorizer is Native
    // re-initialize the authorizer, else for non-native authorizer
    // like ranger we can reuse previous value if it is initialized
    if (null == accessAuthorizer || accessAuthorizer.isNative()) {
      accessAuthorizer = OzoneAuthorizerFactory.forOM(this);
    }

    omMetadataReader = new OmMetadataReader(keyManager, prefixManager,
        this, LOG, AUDIT, metrics, accessAuthorizer);
    // Active DB's OmMetadataReader instance does not need to be reference
    // counted, but it still needs to be wrapped to be consistent.
    rcOmMetadataReader = new ReferenceCounted<>(omMetadataReader, true, null);

    // Reload snapshot feature config flag
    fsSnapshotEnabled = configuration.getBoolean(
        OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY,
        OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_DEFAULT);
    omSnapshotManager = new OmSnapshotManager(this);

    // Snapshot metrics
    updateActiveSnapshotMetrics();

    if (withNewSnapshot) {
      Integer layoutVersionInDB = getLayoutVersionInDB();
      if (layoutVersionInDB != null &&
          versionManager.getMetadataLayoutVersion() < layoutVersionInDB) {
        LOG.info("New OM snapshot received with higher layout version {}. " +
            "Attempting to finalize current OM to that version.",
            layoutVersionInDB);
        OmUpgradeConfig uConf = configuration.getObject(OmUpgradeConfig.class);
        upgradeFinalizer.finalizeAndWaitForCompletion(
            "om-ratis-snapshot", this,
            uConf.getRatisBasedFinalizationTimeout());
        if (versionManager.getMetadataLayoutVersion() < layoutVersionInDB) {
          throw new IOException("Unable to finalize OM to the desired layout " +
              "version " + layoutVersionInDB + " present in the snapshot DB.");
        } else {
          updateLayoutVersionInDB(versionManager, metadataManager);
        }
      }

      instantiatePrepareStateAfterSnapshot();
    } else {
      // Prepare state depends on the transaction ID of metadataManager after a
      // restart.
      instantiatePrepareStateOnStartup();
    }
  }

  /**
   * Return scmClient.
   */
  public ScmClient getScmClient() {
    return scmClient;
  }

  /**
   * Return SecretManager for OM.
   */
  public OzoneBlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenMgr;
  }

  /**
   * Return config value of {@link OzoneConfigKeys#OZONE_SCM_BLOCK_SIZE}.
   */
  public long getScmBlockSize() {
    return scmBlockSize;
  }

  /**
   * Return config value of
   * {@link OzoneConfigKeys#OZONE_KEY_PREALLOCATION_BLOCKS_MAX}.
   */
  public int getPreallocateBlocksMax() {
    return preallocateBlocksMax;
  }

  /**
   * Return config value of
   * {@link HddsConfigKeys#HDDS_BLOCK_TOKEN_ENABLED}.
   */
  public boolean isGrpcBlockTokenEnabled() {
    return grpcBlockTokenEnabled;
  }

  /**
   * Returns true if S3 multi-tenancy is enabled; false otherwise.
   */
  public boolean isS3MultiTenancyEnabled() {
    return isS3MultiTenancyEnabled;
  }

  public boolean isStrictS3() {
    return isStrictS3;
  }

  /**
   * Throws OMException FEATURE_NOT_ENABLED if S3 multi-tenancy is not enabled.
   */
  public void checkS3MultiTenancyEnabled() throws OMException {
    if (isS3MultiTenancyEnabled()) {
      return;
    }

    throw new OMException("S3 multi-tenancy feature is not enabled. Please "
        + "set ozone.om.multitenancy.enabled to true and restart all OMs.",
        FEATURE_NOT_ENABLED);
  }

  /**
   * Return config value of {@link OzoneConfigKeys#OZONE_SECURITY_ENABLED_KEY}.
   */
  public boolean isSecurityEnabled() {
    return isSecurityEnabled || testSecureOmFlag;
  }

  public boolean isTestSecureOmFlag() {
    return testSecureOmFlag;
  }

  private KeyProviderCryptoExtension createKeyProviderExt(
      OzoneConfiguration conf) throws IOException {
    KeyProvider keyProvider = KMSUtil.createKeyProvider(conf,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
    if (keyProvider == null) {
      return null;
    }
    return KeyProviderCryptoExtension
        .createKeyProviderCryptoExtension(keyProvider);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  /**
   * Class which schedule saving metrics to a file.
   */
  private final class ScheduleOMMetricsWriteTask extends TimerTask {
    @Override
    public void run() {
      saveOmMetrics();
    }
  }

  private void saveOmMetrics() {
    try {
      File parent = getTempMetricsStorageFile().getParentFile();
      if (!parent.exists()) {
        Files.createDirectories(parent.toPath());
      }
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(Files.newOutputStream(getTempMetricsStorageFile().toPath()), UTF_8))) {
        OmMetricsInfo metricsInfo = new OmMetricsInfo();
        metricsInfo.setNumKeys(metrics.getNumKeys());
        WRITER.writeValue(writer, metricsInfo);
      }

      Files.move(getTempMetricsStorageFile().toPath(),
          getMetricsStorageFile().toPath(), StandardCopyOption
              .ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ex) {
      LOG.error("Unable to write the om Metrics file", ex);
    }
  }

  /**
   * Returns temporary metrics storage file.
   *
   * @return File
   */
  private File getTempMetricsStorageFile() {
    return new File(omMetaDir, OM_METRICS_TEMP_FILE);
  }

  /**
   * Returns metrics storage file.
   *
   * @return File
   */
  private File getMetricsStorageFile() {
    return new File(omMetaDir, OM_METRICS_FILE);
  }

  private OzoneDelegationTokenSecretManager createDelegationTokenSecretManager(
      OzoneConfiguration conf) throws IOException {
    long tokenRemoverScanInterval =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_KEY,
            OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long tokenMaxLifetime =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
            TimeUnit.MILLISECONDS);
    long tokenRenewInterval =
        conf.getTimeDuration(OMConfigKeys.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            OMConfigKeys.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long certificateGracePeriod = Duration.parse(
        conf.get(HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION,
            HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION_DEFAULT)).toMillis();
    boolean tokenSanityChecksEnabled = conf.getBoolean(
        HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED,
        HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED_DEFAULT);
    if (tokenSanityChecksEnabled && tokenMaxLifetime > certificateGracePeriod) {
      throw new IllegalArgumentException("Certificate grace period " +
          HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION +
          " should be greater than maximum delegation token lifetime " +
          OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY);
    }

    return new OzoneDelegationTokenSecretManager.Builder()
        .setConf(conf)
        .setTokenMaxLifetime(tokenMaxLifetime)
        .setTokenRenewInterval(tokenRenewInterval)
        .setTokenRemoverScanInterval(tokenRemoverScanInterval)
        .setService(omRpcAddressTxt)
        .setOzoneManager(this)
        .setS3SecretManager(s3SecretManager)
        .setCertificateClient(certClient)
        .setSecretKeyClient(secretKeyClient)
        .setOmServiceId(omNodeDetails.getServiceId())
        .build();
  }

  private OzoneBlockTokenSecretManager createBlockTokenSecretManager() {
    long expiryTime = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    return new OzoneBlockTokenSecretManager(expiryTime, secretKeyClient);
  }

  private void stopSecretManager() {
    if (secretKeyClient != null) {
      LOG.info("Stopping secret key client.");
      secretKeyClient.stop();
    }

    if (delegationTokenMgr != null) {
      LOG.info("Stopping OM delegation token secret manager.");
      try {
        delegationTokenMgr.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop delegation token manager", e);
      }
    }
  }

  @Override
  public UUID refetchSecretKey() {
    secretKeyClient.refetchSecretKey();
    return secretKeyClient.getCurrentSecretKey().getId();
  }

  @VisibleForTesting
  public void startSecretManager() {
    try {
      certClient.assertValidKeysAndCertificate();
    } catch (OzoneSecurityException e) {
      LOG.error("Unable to read key pair for OM.", e);
      throw new UncheckedIOException(e);
    }

    if (secConfig.isSecurityEnabled()) {
      LOG.info("Starting secret key client.");
      try {
        secretKeyClient.start(configuration);
      } catch (IOException e) {
        LOG.error("Unable to initialize secret key.", e);
        throw new UncheckedIOException(e);
      }
    }

    if (delegationTokenMgr != null) {
      try {
        LOG.info("Starting OM delegation token secret manager");
        delegationTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting delegation token secret manager.", e);
        throw new UncheckedIOException(e);
      }
    }
  }

  /**
   * For testing purpose only.
   */
  @VisibleForTesting
  public void setCertClient(CertificateClient newClient) throws IOException {
    if (certClient != null) {
      certClient.close();
    }
    certClient = newClient;
    serviceInfo = new ServiceInfoProvider(secConfig, this, certClient);
  }

  /**
   * For testing purpose only. This allows setting up ScmBlockLocationClient
   * without having to fully setup a working cluster.
   */
  @VisibleForTesting
  public void setScmTopologyClient(
      ScmTopologyClient scmTopologyClient) {
    this.scmTopologyClient = scmTopologyClient;
  }

  public NetworkTopology getClusterMap() {
    return scmTopologyClient.getClusterMap();
  }

  /**
   * For testing purpose only. This allows testing token in integration test
   * without fully setting up a working secure cluster.
   */
  @VisibleForTesting
  public void setSecretKeyClient(SecretKeyClient secretKeyClient) {
    this.secretKeyClient = secretKeyClient;
    if (blockTokenMgr != null) {
      blockTokenMgr.setSecretKeyClient(secretKeyClient);
    }
    if (delegationTokenMgr != null) {
      delegationTokenMgr.setSecretKeyClient(secretKeyClient);
    }
  }

  /**
   * Login OM service user if security and Kerberos are enabled.
   */
  private static void loginOMUser(OzoneConfiguration conf)
      throws IOException, AuthenticationException {

    if (SecurityUtil.getAuthenticationMethod(conf).equals(
        AuthenticationMethod.KERBEROS)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ozone security is enabled. Attempting login for OM user. "
                + "Principal: {}, keytab: {}", conf.get(
            OZONE_OM_KERBEROS_PRINCIPAL_KEY),
            conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY));
      }

      UserGroupInformation.setConfiguration(conf);

      InetSocketAddress socAddr = OmUtils.getOmAddress(conf);
      SecurityUtil.login(conf, OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
          OZONE_OM_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
    } else {
      throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
          conf) + " authentication method not supported. OM user login "
          + "failed.");
    }
    LOG.info("Ozone Manager login successful.");
  }

  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @return {@link ScmBlockLocationProtocol}
   */
  private static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) {
    return HAUtils.getScmBlockClient(conf);
  }

  /**
   * Returns a scm container client.
   *
   * @return {@link StorageContainerLocationProtocol}
   */
  private static StorageContainerLocationProtocol getScmContainerClient(
      OzoneConfiguration conf) {
    return HAUtils.getScmContainerClient(conf);
  }

  /**
   * Creates a new instance of rpc server. If an earlier instance is already
   * running then returns the same.
   */
  private RPC.Server getRpcServer(OzoneConfiguration conf) throws IOException {
    if (isOmRpcServerRunning) {
      return omRpcServer;
    }

    LOG.info("Creating RPC Server");
    InetSocketAddress omNodeRpcAddr = OmUtils.getOmAddress(conf);
    boolean flexibleFqdnResolutionEnabled = conf.getBoolean(
            OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED,
            OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT);
    if (flexibleFqdnResolutionEnabled && omNodeRpcAddr.getAddress() == null) {
      omNodeRpcAddr =
              OzoneNetUtils.getAddressWithHostNameLocal(omNodeRpcAddr);
    }

    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omServerProtocol = new OzoneManagerProtocolServerSideTranslatorPB(
        this, omRatisServer, omClientProtocolMetrics);
    BlockingService omService =
        OzoneManagerService.newReflectiveBlockingService(omServerProtocol);

    OMInterServiceProtocolServerSideImpl omInterServerProtocol =
        new OMInterServiceProtocolServerSideImpl(this, omRatisServer);
    BlockingService omInterService =
        OzoneManagerInterService.newReflectiveBlockingService(
            omInterServerProtocol);

    OMAdminProtocolServerSideImpl omMetadataServerProtocol =
        new OMAdminProtocolServerSideImpl(this);
    BlockingService omAdminService =
        OzoneManagerAdminService.newReflectiveBlockingService(
            omMetadataServerProtocol);

    ReconfigureProtocolServerSideTranslatorPB reconfigureServerProtocol
        = new ReconfigureProtocolServerSideTranslatorPB(reconfigurationHandler);
    BlockingService reconfigureService =
        ReconfigureProtocolService.newReflectiveBlockingService(
            reconfigureServerProtocol);

    return startRpcServer(conf, omNodeRpcAddr, omService,
        omInterService, omAdminService, reconfigureService);
  }

  /**
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param clientProtocolService RPC protocol for client communication
   *                              (OzoneManagerProtocolPB impl)
   * @param interOMProtocolService RPC protocol for inter OM communication
   *                               (OMInterServiceProtocolPB impl)
   * @param reconfigureProtocolService RPC protocol for reconfigure
   *    *                              (ReconfigureProtocolPB impl)
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, BlockingService clientProtocolService,
      BlockingService interOMProtocolService,
      BlockingService omAdminProtocolService,
      BlockingService reconfigureProtocolService)
      throws IOException {

    final int handlerCount = conf.getInt(OZONE_OM_HANDLER_COUNT_KEY,
        OZONE_OM_HANDLER_COUNT_DEFAULT);
    final int readThreads = conf.getInt(OZONE_OM_READ_THREADPOOL_KEY,
        OZONE_OM_READ_THREADPOOL_DEFAULT);

    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(OzoneManagerProtocolPB.class)
        .setInstance(clientProtocolService)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setNumReaders(readThreads)
        .setVerbose(false)
        .setSecretManager(delegationTokenMgr)
        .build();

    HddsServerUtil.addPBProtocol(conf, OMInterServiceProtocolPB.class,
        interOMProtocolService, rpcServer);
    HddsServerUtil.addPBProtocol(conf, OMAdminProtocolPB.class,
        omAdminProtocolService, rpcServer);
    HddsServerUtil.addPBProtocol(conf, ReconfigureProtocolOmPB.class,
        reconfigureProtocolService, rpcServer);

    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      rpcServer.refreshServiceAcl(conf, OMPolicyProvider.getInstance());
    }

    rpcServer.addSuppressedLoggingExceptions(OMNotLeaderException.class,
        OMLeaderNotReadyException.class);

    return rpcServer;
  }

  /**
   * Starts an s3g OmGrpc server.
   *
   * @param conf         configuration
   * @return gRPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private GrpcOzoneManagerServer startGrpcServer(OzoneConfiguration conf) {
    return new GrpcOzoneManagerServer(conf,
        this.omServerProtocol,
        this.delegationTokenMgr,
        this.certClient,
        this.threadPrefix);
  }

  private static boolean isOzoneSecurityEnabled() {
    return securityEnabled;
  }

  /**
   * Logs in the OM user if security is enabled in the configuration.
   *
   * @param conf OzoneConfiguration
   * @throws IOException, AuthenticationException in case login fails.
   */
  private static void loginOMUserIfSecurityEnabled(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    securityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (securityEnabled && testUgi == null) {
      // Checking certificate duration validity by using
      // validateCertificateValidityConfig() in SecurityConfig constructor.
      new SecurityConfig(conf);
      loginOMUser(conf);
    }
  }

  /**
   * Initializes the OM instance.
   *
   * @param conf OzoneConfiguration
   * @return true if OM initialization succeeds, false otherwise
   * @throws IOException in case ozone metadata directory path is not
   *                     accessible
   */
  @VisibleForTesting
  public static boolean omInit(OzoneConfiguration conf) throws IOException,
      AuthenticationException {
    OMHANodeDetails omhaNodeDetails = OMHANodeDetails.loadOMHAConfig(conf);
    String nodeId = omhaNodeDetails.getLocalNodeDetails().getNodeId();
    loginOMUserIfSecurityEnabled(conf);
    OMStorage omStorage = new OMStorage(conf);
    StorageState state = omStorage.getState();
    String scmId;
    try {
      ScmInfo scmInfo = getScmInfo(conf);
      scmId = scmInfo.getScmId();
      if (scmId == null || scmId.isEmpty()) {
        throw new IOException("Invalid SCM ID");
      }
      String clusterId = scmInfo.getClusterId();
      if (clusterId == null || clusterId.isEmpty()) {
        throw new IOException("Invalid Cluster ID");
      }

      if (state != StorageState.INITIALIZED) {
        omStorage.setOmNodeId(nodeId);
        omStorage.setClusterId(clusterId);
        omStorage.initialize();
        System.out.println(
            "OM initialization succeeded.Current cluster id for sd="
                + omStorage.getStorageDir() + ";cid=" + omStorage
                .getClusterID() + ";layoutVersion=" + omStorage
                .getLayoutVersion());
      } else {
        System.out.println(
            "OM already initialized.Reusing existing cluster id for sd="
                + omStorage.getStorageDir() + ";cid=" + omStorage
                .getClusterID() + ";layoutVersion=" + omStorage
                .getLayoutVersion());
      }
    } catch (IOException ioe) {
      LOG.error("Could not initialize OM version file", ioe);
      return false;
    }

    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      LOG.info("OM storage initialized. Initializing security");
      initializeSecurity(conf, omStorage, scmId);
    }
    omStorage.persistCurrentState();
    return true;
  }

  /**
   * Initializes secure OzoneManager.
   */
  @VisibleForTesting
  public static void initializeSecurity(OzoneConfiguration conf,
      OMStorage omStore, String scmId) throws IOException {
    LOG.info("Initializing secure OzoneManager.");

    HddsProtos.OzoneManagerDetailsProto omInfo =
        getOmDetailsProto(conf, omStore.getOmId());

    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        getScmSecurityClientWithMaxRetry(conf, getCurrentUser());

    OMCertificateClient certClient =
        new OMCertificateClient(
            new SecurityConfig(conf), scmSecurityClient, omStore, omInfo,
            "", scmId,
            certId -> {
              try {
                omStore.setOmCertSerialId(certId);
              } catch (IOException e) {
                LOG.error("Failed to set new certificate ID", e);
                throw new RuntimeException("Failed to set new certificate ID");
              }
            }, null);
    certClient.initWithRecovery();
  }

  private void initializeRatisDirs(OzoneConfiguration conf) throws IOException {
    // Create Ratis storage dir
    String omRatisDirectory =
        OzoneManagerRatisUtils.getOMRatisDirectory(conf);
    if (omRatisDirectory == null || omRatisDirectory.isEmpty()) {
      throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
          " must be defined.");
    }
    OmUtils.createOMDir(omRatisDirectory);

    String scmStorageDir = SCMHAUtils.getRatisStorageDir(conf);
    if (!Strings.isNullOrEmpty(omRatisDirectory) && !Strings
        .isNullOrEmpty(scmStorageDir) && omRatisDirectory
        .equals(scmStorageDir)) {
      throw new IOException(
          "Path of " + OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR + " and "
              + ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR
              + " should not be co located. Please change atleast one path.");
    }

    // Create Ratis snapshot dir
    omRatisSnapshotDir = OmUtils.createOMDir(
        OzoneManagerRatisUtils.getOMRatisSnapshotDirectory(conf));

    // Before starting ratis server, check if previous installation has
    // snapshot directory in Ratis storage directory. if yes, move it to
    // new snapshot directory.

    File snapshotDir = new File(omRatisDirectory, OZONE_RATIS_SNAPSHOT_DIR);

    if (snapshotDir.isDirectory()) {
      FileUtils.moveDirectory(snapshotDir.toPath(),
          omRatisSnapshotDir.toPath());
    }

    File omRatisDir = new File(omRatisDirectory);
    String groupIDfromServiceID = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(getOMServiceId())).getUuid().toString();

    // If a directory exists in ratis storage dir
    // Check the Ratis group Dir is same as the one generated from
    // om service id.

    // This will help to catch if some one has changed service id later on.
    File[] ratisDirFiles = omRatisDir.listFiles();
    if (ratisDirFiles != null) {
      for (File ratisGroupDir : ratisDirFiles) {
        if (ratisGroupDir.isDirectory()) {
          if (!ratisGroupDir.getName().equals(groupIDfromServiceID)) {
            throw new IOException("Ratis group Dir on disk "
                + ratisGroupDir.getName() + " does not match with RaftGroupID"
                + groupIDfromServiceID + " generated from service id "
                + getOMServiceId() + ". Looks like there is a change to " +
                OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY + " value after the " +
                "cluster is setup. Currently change to this value is not " +
                "supported.");
          }
        } else {
          LOG.warn("Unknown file {} exists in ratis storage dir {}."
              + " It is recommended not to share the ratis storage dir.",
              ratisGroupDir, omRatisDir);
        }
      }
    }

    if (peerNodesMap != null && !peerNodesMap.isEmpty()) {
      this.omRatisSnapshotProvider = new OmRatisSnapshotProvider(
          configuration, omRatisSnapshotDir, peerNodesMap);
    }
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr        RPC server listening address
   * @return server startup message
   */
  private static String buildRpcServerStartMessage(String description,
      InetSocketAddress addr) {
    return addr != null
        ? String.format("%s is listening at %s", description, addr)
        : String.format("%s not started", description);
  }

  @VisibleForTesting
  public KeyManager getKeyManager() {
    return keyManager;
  }

  @VisibleForTesting
  public OMStorage getOmStorage() {
    return omStorage;
  }

  @VisibleForTesting
  public OzoneManagerRatisServer getOmRatisServer() {
    return omRatisServer;
  }

  @VisibleForTesting
  public OmRatisSnapshotProvider getOmSnapshotProvider() {
    return omRatisSnapshotProvider;
  }

  @VisibleForTesting
  public InetSocketAddress getOmRpcServerAddr() {
    return omRpcAddress;
  }

  @VisibleForTesting
  public LifeCycle.State getOmRatisServerState() {
    if (omRatisServer == null) {
      return null;
    } else {
      return omRatisServer.getServerState();
    }
  }

  @VisibleForTesting
  public KeyProviderCryptoExtension getKmsProvider() {
    return kmsProvider;
  }

  public PrefixManager getPrefixManager() {
    return prefixManager;
  }

  public VolumeManager getVolumeManager() {
    return volumeManager;
  }

  public BucketManager getBucketManager() {
    return bucketManager;
  }

  public IAccessAuthorizer getAccessAuthorizer() {
    return accessAuthorizer;
  }

  /**
   * Get metadata manager.
   *
   * @return metadata manager.
   */
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public S3SecretManager getS3SecretManager() {
    return s3SecretManager;
  }

  /**
   * Get snapshot manager.
   *
   * @return Om snapshot manager.
   */
  public OmSnapshotManager getOmSnapshotManager() {
    return omSnapshotManager;
  }

  /**
   * Get metadata manager.
   *
   * @return metadata manager.
   */
  public OMMultiTenantManager getMultiTenantManager() {
    return multiTenantManager;
  }

  public OzoneBlockTokenSecretManager getBlockTokenMgr() {
    return blockTokenMgr;
  }

  public OzoneManagerProtocolServerSideTranslatorPB getOmServerProtocol() {
    return omServerProtocol;
  }

  public OMMetrics getMetrics() {
    return metrics;
  }

  public OMPerformanceMetrics getPerfMetrics() {
    return perfMetrics;
  }
  public DeletingServiceMetrics getDeletionMetrics() {
    return omDeletionMetrics;
  }

  public OzoneTrash getOzoneTrash() {
    return ozoneTrash;
  }
  /**
   * Start service.
   */
  public void start() throws IOException {
    Map<String, String> auditMap = new HashMap();
    auditMap.put("OmState", omState.name());
    if (omState == State.BOOTSTRAPPING) {
      if (isBootstrapping) {
        auditMap.put("Bootstrap", "normal");
        // Check that all OM configs have been updated with the new OM info.
        checkConfigBeforeBootstrap();
      } else if (isForcedBootstrapping) {
        auditMap.put("Bootstrap", "force");
        LOG.warn("Skipped checking whether existing OM configs have been " +
            "updated with this OM information as force bootstrap is called.");
      }
    }

    omClientProtocolMetrics.register();
    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    metadataManager.start(configuration);

    startSecretManagerIfNecessary();
    // Start Ratis services
    if (omRatisServer != null) {
      omRatisServer.start();
    }

    upgradeFinalizer.runPrefinalizeStateActions(omStorage, this);
    Integer layoutVersionInDB = getLayoutVersionInDB();
    if (layoutVersionInDB == null ||
        versionManager.getMetadataLayoutVersion() != layoutVersionInDB) {
      LOG.info("Version File has different layout " +
              "version ({}) than OM DB ({}). That is expected if this " +
              "OM has never been finalized to a newer layout version.",
          versionManager.getMetadataLayoutVersion(), layoutVersionInDB);
    }

    metrics.setNumVolumes(metadataManager
        .countEstimatedRowsInTable(metadataManager.getVolumeTable()));
    metrics.setNumBuckets(metadataManager
        .countEstimatedRowsInTable(metadataManager.getBucketTable()));

    if (getMetricsStorageFile().exists()) {
      OmMetricsInfo metricsInfo = READER.readValue(getMetricsStorageFile());
      metrics.setNumKeys(metricsInfo.getNumKeys());
    }

    // FSO(FILE_SYSTEM_OPTIMIZED)
    metrics.setNumDirs(metadataManager
        .countEstimatedRowsInTable(metadataManager.getDirectoryTable()));
    metrics.setNumFiles(metadataManager
        .countEstimatedRowsInTable(metadataManager.getFileTable()));

    // Schedule save metrics
    long period = configuration.getTimeDuration(OZONE_OM_METRICS_SAVE_INTERVAL,
        OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    scheduleOMMetricsWriteTask = new ScheduleOMMetricsWriteTask();
    metricsTimer = new Timer();
    metricsTimer.schedule(scheduleOMMetricsWriteTask, 0, period);

    try {
      scmTopologyClient.start(configuration);
    } catch (IOException ex) {
      LOG.error("Unable to initialize network topology schema file. ", ex);
      throw new UncheckedIOException(ex);
    }

    keyManager.start(configuration);

    try {
      httpServer = new OzoneManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // Allow OM to start as Http Server failure is not fatal.
      LOG.error("OM HttpServer failed to start.", ex);
    }

    omRpcServer.start();
    isOmRpcServerRunning = true;

    startTrashEmptier(configuration);
    if (isOmGrpcServerEnabled) {
      omS3gGrpcServer.start();
      isOmGrpcServerRunning = true;
    }
    registerMXBean();

    setStartTime();

    if (omState == State.BOOTSTRAPPING) {
      bootstrap(omNodeDetails);
    }

    omState = State.RUNNING;
    auditMap.put("NewOmState", omState.name());
    SYSTEMAUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMSystemAction.STARTUP, auditMap));
  }

  /**
   * Restarts the service. This method re-initializes the rpc server.
   */
  public void restart() throws IOException {
    Map<String, String> auditMap = new HashMap();
    auditMap.put("OmState", omState.name());
    auditMap.put("Trigger", "restart");
    setInstanceVariablesFromConf();

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    instantiateServices(false);

    metadataManager.start(configuration);
    keyManager.start(configuration);
    startSecretManagerIfNecessary();

    // Set metrics and start metrics back ground thread
    metrics.setNumVolumes(metadataManager.countRowsInTable(metadataManager
        .getVolumeTable()));
    metrics.setNumBuckets(metadataManager.countRowsInTable(metadataManager
        .getBucketTable()));

    if (getMetricsStorageFile().exists()) {
      OmMetricsInfo metricsInfo = READER.readValue(getMetricsStorageFile());
      metrics.setNumKeys(metricsInfo.getNumKeys());
    }

    // FSO(FILE_SYSTEM_OPTIMIZED)
    metrics.setNumDirs(metadataManager
        .countEstimatedRowsInTable(metadataManager.getDirectoryTable()));
    metrics.setNumFiles(metadataManager
        .countEstimatedRowsInTable(metadataManager.getFileTable()));

    // Schedule save metrics
    long period = configuration.getTimeDuration(OZONE_OM_METRICS_SAVE_INTERVAL,
        OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    scheduleOMMetricsWriteTask = new ScheduleOMMetricsWriteTask();
    metricsTimer = new Timer();
    metricsTimer.schedule(scheduleOMMetricsWriteTask, 0, period);

    initializeRatisServer(false);
    if (omRatisServer != null) {
      omRatisServer.start();
    }

    omRpcServer = getRpcServer(configuration);
    if (isOmGrpcServerEnabled) {
      omS3gGrpcServer = getOmS3gGrpcServer(configuration);
    }
    try {
      httpServer = new OzoneManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // Allow OM to start as Http Server failure is not fatal.
      LOG.error("OM HttpServer failed to start.", ex);
    }
    omRpcServer.start();
    isOmRpcServerRunning = true;

    startTrashEmptier(configuration);
    registerMXBean();

    if (isOmGrpcServerEnabled) {
      omS3gGrpcServer.start();
      isOmGrpcServerRunning = true;
    }
    setStartTime();
    omState = State.RUNNING;
    auditMap.put("NewOmState", omState.name());
    SYSTEMAUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMSystemAction.STARTUP, auditMap));
  }

  /**
   * Iterate the Snapshot table, check the status
   * for every snapshot and update OMMetrics.
   */
  private void updateActiveSnapshotMetrics()
      throws IOException {

    long activeGauge = 0;
    long deletedGauge = 0;

    try (TableIterator<String, ? extends
        KeyValue<String, SnapshotInfo>> keyIter =
             metadataManager.getSnapshotInfoTable().iterator()) {

      while (keyIter.hasNext()) {
        SnapshotInfo info = keyIter.next().getValue();

        SnapshotInfo.SnapshotStatus snapshotStatus =
            info.getSnapshotStatus();

        if (snapshotStatus == SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
          activeGauge++;
        } else if (snapshotStatus ==
            SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
          deletedGauge++;
        }
      }
    }

    metrics.setNumSnapshotActive(activeGauge);
    metrics.setNumSnapshotDeleted(deletedGauge);
  }

  private void checkConfigBeforeBootstrap() throws IOException {
    List<OMNodeDetails> omsWithoutNewConfig = new ArrayList<>();
    for (Map.Entry<String, OMNodeDetails> entry : peerNodesMap.entrySet()) {
      String remoteNodeId = entry.getKey();
      OMNodeDetails remoteNodeDetails = entry.getValue();
      try (OMAdminProtocolClientSideImpl omAdminProtocolClient =
               OMAdminProtocolClientSideImpl.createProxyForSingleOM(
                   configuration, getRemoteUser(), entry.getValue())) {

        OMConfiguration remoteOMConfiguration =
            omAdminProtocolClient.getOMConfiguration();
        checkRemoteOMConfig(remoteNodeId, remoteOMConfiguration);
      } catch (IOException ioe) {
        LOG.error("Remote OM config check failed on OM {}", remoteNodeId, ioe);
        omsWithoutNewConfig.add(remoteNodeDetails);
      }
    }
    if (!omsWithoutNewConfig.isEmpty()) {
      String errorMsg = OmUtils.getOMAddressListPrintString(omsWithoutNewConfig)
          + " do not have or have incorrect information of the bootstrapping " +
          "OM. Update their ozone-site.xml before proceeding.";
      exitManager.exitSystem(1, errorMsg, LOG);
    }
  }

  /**
   * Verify that the remote OM configuration is updated for the bootstrapping
   * OM.
   */
  private void checkRemoteOMConfig(String remoteNodeId,
      OMConfiguration remoteOMConfig) throws IOException {
    if (remoteOMConfig == null) {
      throw new IOException("Remote OM " + remoteNodeId + " configuration " +
          "returned null");
    }

    if (remoteOMConfig.getCurrentPeerList().contains(this.getOMNodeId())) {
      LOG.warn(
          "Remote OM {} already contains bootstrapping OM({}) as part of "
              + "its Raft group peers.",
          remoteNodeId, getOMNodeId());
    }

    OMNodeDetails omNodeDetailsInRemoteConfig = remoteOMConfig
        .getActiveOmNodesInNewConf().get(getOMNodeId());
    if (omNodeDetailsInRemoteConfig == null) {
      throw new IOException("Remote OM " + remoteNodeId + " does not have the" +
          " bootstrapping OM(" + getOMNodeId() + ") information on reloading " +
          "configs or it could not resolve the address.");
    }

    if (!omNodeDetailsInRemoteConfig.getRpcAddress().equals(
        this.omNodeDetails.getRpcAddress())) {
      throw new IOException("Remote OM " + remoteNodeId + " configuration has" +
          " bootstrapping OM(" + getOMNodeId() + ") address as " +
          omNodeDetailsInRemoteConfig.getRpcAddress() + " where the " +
          "bootstrapping OM address is " + omNodeDetails.getRpcAddress());
    }

    if (omNodeDetailsInRemoteConfig.isDecommissioned()) {
      throw new IOException("Remote OM " + remoteNodeId + " configuration has" +
          " bootstrapping OM(" + getOMNodeId() + ") in decommissioned " +
          "nodes list - " + OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY);
    }
  }

  @Override
  public void bootstrap(OMNodeDetails newOMNode) throws IOException {
    // Create InterOmServiceProtocol client to send request to other OMs
    try (OMInterServiceProtocolClientSideImpl omInterServiceProtocol =
             new OMInterServiceProtocolClientSideImpl(configuration,
                 getRemoteUser(), getOMServiceId())) {

      omInterServiceProtocol.bootstrap(omNodeDetails);

      LOG.info("Successfully bootstrapped OM {} and joined the Ratis group " +
          "{}", getOMNodeId(), omRatisServer.getRaftGroup());
    } catch (Exception e) {
      LOG.error("Failed to Bootstrap OM.");
      throw e;
    }
  }

  /**
   * When OMStateMachine receives a configuration change update, it calls
   * this function to update the peers list, if required. The configuration
   * change could be to add or to remove an OM from the ring.
   */
  public void updatePeerList(List<String> newPeers) {
    final Set<String> currentPeers = omRatisServer.getPeerIds();

    // NodeIds present in new node list and not in current peer list are the
    // bootstapped OMs and should be added to the peer list
    List<String> bootstrappedOMs = new ArrayList<>(newPeers);
    bootstrappedOMs.removeAll(currentPeers);

    // NodeIds present in current peer list but not in new node list are the
    // decommissioned OMs and should be removed from the peer list
    List<String> decommissionedOMs = new ArrayList<>(currentPeers);
    decommissionedOMs.removeAll(newPeers);

    // Add bootstrapped OMs to peer list
    for (String omNodeId : bootstrappedOMs) {
      // Check if its the local nodeId (bootstrapping OM)
      if (isCurrentNode(omNodeId)) {
        // For a Bootstrapping OM, none of the peers are added to it's
        // RatisServer's peer list and it needs to be updated here after
        // receiving the conf change notification from Ratis.
        for (String peerNodeId : newPeers) {
          if (peerNodeId.equals(omNodeId)) {
            omRatisServer.addRaftPeer(omNodeDetails);
          } else {
            omRatisServer.addRaftPeer(peerNodesMap.get(peerNodeId));
          }
        }
      } else {
        // For other nodes, add bootstrapping OM to OM peer list (which
        // internally adds to Ratis peer list too)
        try {
          addOMNodeToPeers(omNodeId);
        } catch (IOException e) {
          LOG.error("Fatal Error while adding bootstrapped node to " +
              "peer list. Shutting down the system as otherwise it " +
              "could lead to OM state divergence.", e);
          exitManager.forceExit(1, e, LOG);
        }
      }
    }

    // Remove decommissioned OMs from peer list
    for (String omNodeId : decommissionedOMs) {
      if (isCurrentNode(omNodeId)) {
        // Decommissioning Node should not receive the configuration change
        // request. It may receive the request if the newly added node id or
        // the decommissioned node id is same.
        LOG.warn("New OM node Id: {} is same as decommissioned earlier",
            omNodeId);
      } else {
        // Remove decommissioned node from peer list (which internally
        // removed from Ratis peer list too)
        try {
          removeOMNodeFromPeers(omNodeId);
        } catch (IOException e) {
          LOG.error("Fatal Error while removing decommissioned node from " +
              "peer list. Shutting down the system as otherwise it " +
              "could lead to OM state divergence.", e);
          exitManager.forceExit(1, e, LOG);
        }
      }
    }
  }

  /**
   * Check if the given nodeId is the current nodeId.
   */
  private boolean isCurrentNode(String omNodeID) {
    return getOMNodeId().equals(omNodeID);
  }

  /**
   * Add an OM Node to the peers list. This call comes from OMStateMachine
   * after a SetConfiguration request has been successfully executed by the
   * Ratis server.
   */
  private void addOMNodeToPeers(String newOMNodeId) throws IOException {
    OMNodeDetails newOMNodeDetails = null;
    try {
      newOMNodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(
          getConfiguration(), getOMServiceId(), newOMNodeId);
      if (newOMNodeDetails == null) {
        // Load new configuration object to read in new peer information
        setConfiguration(reloadConfiguration());
        newOMNodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(
            getConfiguration(), getOMServiceId(), newOMNodeId);

        if (newOMNodeDetails == null) {
          // If new node information is not present in the newly loaded
          // configuration also, throw an exception.
          // This case can also come when we have decommissioned a node and
          // ratis will apply previous transactions to add that node back.
          LOG.error(
              "There is no OM configuration for node ID {} in ozone-site.xml.",
              newOMNodeId);
          return;
        }
      }
    } catch (IOException e) {
      LOG.error("{}: Couldn't add OM {} to peer list.", getOMNodeId(),
          newOMNodeId);
      return;
    }

    if (omRatisSnapshotProvider == null) {
      omRatisSnapshotProvider = new OmRatisSnapshotProvider(
          configuration, omRatisSnapshotDir, peerNodesMap);
    } else {
      omRatisSnapshotProvider.addNewPeerNode(newOMNodeDetails);
    }
    omRatisServer.addRaftPeer(newOMNodeDetails);
    peerNodesMap.put(newOMNodeId, newOMNodeDetails);
    LOG.info("Added OM {} to the Peer list.", newOMNodeId);
  }

  /**
   * Remove an OM Node from the peers list. This call comes from OMStateMachine
   * after a SetConfiguration request has been successfully executed by the
   * Ratis server.
   */
  private void removeOMNodeFromPeers(String decommNodeId) throws IOException {
    OMNodeDetails decommOMNodeDetails = peerNodesMap.get(decommNodeId);
    if (decommOMNodeDetails == null) {
      throw new IOException("Decommissioned Node " + decommNodeId + " not " +
          "present in peer list");
    }

    omRatisSnapshotProvider.removeDecommissionedPeerNode(decommNodeId);
    omRatisServer.removeRaftPeer(decommOMNodeDetails);
    peerNodesMap.remove(decommNodeId);
    LOG.info("Removed OM {} from OM Peer Nodes.", decommNodeId);
  }

  /**
   * Check if the input nodeId exists in the peers list.
   * @return true if the nodeId is self or it exists in peer node list,
   *         false otherwise.
   */
  @VisibleForTesting
  public boolean doesPeerExist(String omNodeId) {
    if (getOMNodeId().equals(omNodeId)) {
      return true;
    }
    if (peerNodesMap != null && !peerNodesMap.isEmpty()) {
      return peerNodesMap.containsKey(omNodeId);
    }
    return false;
  }

  /**
   * Return list of all current OM peers (does not reload configuration from
   * disk to find newly configured OMs).
   */
  public List<OMNodeDetails> getAllOMNodesInMemory() {
    List<OMNodeDetails> peerNodes = getPeerNodes();
    // Add current node also to list
    peerNodes.add(omNodeDetails);
    return peerNodes;
  }

  /**
   * Reload configuration from disk and return all the OM nodes present in
   * the new conf under current serviceId.
   */
  public List<OMNodeDetails> getAllOMNodesInNewConf() {
    OzoneConfiguration newConf = reloadConfiguration();
    List<OMNodeDetails> allOMNodeDetails = OmUtils.getAllOMHAAddresses(
        newConf, getOMServiceId(), true);
    if (allOMNodeDetails.isEmpty()) {
      // There are no addresses configured for HA. Return only current OM
      // details.
      return Collections.singletonList(omNodeDetails);
    }
    return allOMNodeDetails;
  }

  /**
   * Starts a Trash Emptier thread that does an fs.trashRoots and performs
   * checkpointing & deletion.
   */
  private void startTrashEmptier(Configuration conf) throws IOException {
    if (emptier == null) {
      float hadoopTrashInterval =
          conf.getFloat(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
      // check whether user has configured ozone specific trash-interval
      // if not fall back to hadoop configuration
      long trashInterval =
          (long) (conf.getFloat(
              OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, hadoopTrashInterval)
              * MSECS_PER_MINUTE);
      if (trashInterval == 0) {
        LOG.info("Trash Interval set to 0. Files deleted won't move to trash");
        return;
      } else if (trashInterval < 0) {
        throw new IOException("Cannot start trash emptier with negative " +
            "interval. Set " + FS_TRASH_INTERVAL_KEY + " to a positive value.");
      }

      OzoneManager i = this;
      FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>)
              () -> new TrashOzoneFileSystem(i));
      this.ozoneTrash = new OzoneTrash(fs, conf, this);
      this.emptier = new Thread(ozoneTrash.getEmptier(), threadPrefix + "TrashEmptier");
      this.emptier.setDaemon(true);
      this.emptier.start();
    }
  }

  /**
   * Creates a new instance of gRPC OzoneManagerServiceGrpc transport server
   * for serving s3g OmRequests.  If an earlier instance is already running
   * then returns the same.
   */
  private GrpcOzoneManagerServer getOmS3gGrpcServer(OzoneConfiguration conf) {
    if (isOmGrpcServerRunning) {
      return omS3gGrpcServer;
    }
    return startGrpcServer(configuration);
  }

  /**
   * Creates an instance of ratis server.
   * @param shouldBootstrap If OM is started in Bootstrap mode, then Ratis
   *                        server will be initialized without adding self to
   *                        Ratis group
   */
  private void initializeRatisServer(boolean shouldBootstrap)
      throws IOException {
    if (omRatisServer == null) {
      // This needs to be done before initializing Ratis.
      ratisReporterList = RatisDropwizardExports.
          registerRatisMetricReporters(ratisMetricsMap, this::isStopped);
      omRatisServer = OzoneManagerRatisServer.newOMRatisServer(
          configuration, this, omNodeDetails, peerNodesMap,
          secConfig, certClient, shouldBootstrap);
    }
    LOG.info("OzoneManager Ratis server initialized at port {}",
        omRatisServer.getServerPort());
  }

  public long getObjectIdFromTxId(long trxnId) {
    return OmUtils.getObjectIdFromTxId(metadataManager.getOmEpoch(),
        trxnId);
  }

  /**
   *
   * @return Gets the stored layout version from the DB meta table.
   * @throws IOException on Error.
   */
  private Integer getLayoutVersionInDB() throws IOException {
    String layoutVersion =
        metadataManager.getMetaTable().get(LAYOUT_VERSION_KEY);
    return (layoutVersion == null) ? null : Integer.parseInt(layoutVersion);
  }

  public TransactionInfo getTransactionInfo() {
    return omTransactionInfo.get();
  }

  public void setTransactionInfo(TransactionInfo info) {
    omTransactionInfo.set(info);
  }

  public long getRatisSnapshotIndex() throws IOException {
    TransactionInfo dbTxnInfo =
        TransactionInfo.readTransactionInfo(metadataManager);
    if (dbTxnInfo == null) {
      // If there are no transactions in the database, it has applied index 0
      // only.
      return 0;
    } else {
      return dbTxnInfo.getTransactionIndex();
    }
  }

  /**
   * Stop service.
   */
  public boolean stop() {
    LOG.info("{}: Stopping Ozone Manager", omNodeDetails.getOMPrintInfo());
    if (isStopped()) {
      return false;
    }
    try {
      omState = State.STOPPED;
      // Cancel the metrics timer and set to null.
      if (metricsTimer != null) {
        metricsTimer.cancel();
        metricsTimer = null;
        scheduleOMMetricsWriteTask = null;
      }
      omRpcServer.stop();
      if (isOmGrpcServerEnabled) {
        omS3gGrpcServer.stop();
      }
      // When ratis is not enabled, we need to call stop() to stop
      // OzoneManageDoubleBuffer in OM server protocol.
      if (omRatisServer != null) {
        omRatisServer.stop();
        OMHAMetrics.unRegister();
      }
      isOmRpcServerRunning = false;
      if (isOmGrpcServerEnabled) {
        isOmGrpcServerRunning = false;
      }
      keyManager.stop();
      stopSecretManager();

      if (scmTopologyClient != null) {
        scmTopologyClient.stop();
      }

      if (httpServer != null) {
        httpServer.stop();
      }
      if (multiTenantManager != null) {
        multiTenantManager.stop();
      }
      stopTrashEmptier();
      metadataManager.stop();
      omSnapshotManager.close();
      metrics.unRegister();
      omClientProtocolMetrics.unregister();
      unregisterMXBean();
      if (omRatisSnapshotProvider != null) {
        omRatisSnapshotProvider.close();
      }
      DeletingServiceMetrics.unregister();
      OMPerformanceMetrics.unregister();
      RatisDropwizardExports.clear(ratisMetricsMap, ratisReporterList);
      scmClient.close();
      if (certClient != null) {
        certClient.close();
      }

      if (omhaMetrics != null) {
        OMHAMetrics.unRegister();
      }
      omRatisServer = null;

      if (bucketUtilizationMetrics != null) {
        bucketUtilizationMetrics.unRegister();
      }

      if (versionManager != null) {
        versionManager.close();
      }

      if (edekCacheLoader != null) {
        edekCacheLoader.shutdown();
      }
      return true;
    } catch (Exception e) {
      LOG.error("OzoneManager stop failed.", e);
    }
    return false;
  }

  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(0, message, LOG);
  }

  public void terminateOM() {
    stop();
    terminate(1);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      omRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during OzoneManager join.", e);
    }
  }

  private void startSecretManagerIfNecessary() {
    boolean shouldRun = isOzoneSecurityEnabled();
    if (shouldRun) {
      boolean running = delegationTokenMgr.isRunning();
      if (!running) {
        startSecretManager();
      }
    }
  }

  /**
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    return !UserGroupInformation.isSecurityEnabled()
        || (authMethod == AuthenticationMethod.KERBEROS)
        || (authMethod == AuthenticationMethod.KERBEROS_SSL)
        || (authMethod == AuthenticationMethod.CERTIFICATE);
  }

  /**
   * Returns authentication method used to establish the connection.
   *
   * @return AuthenticationMethod used to establish connection
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = getRemoteUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   * Get delegation token from OzoneManager.
   *
   * @param renewer Renewer information
   * @return delegationToken DelegationToken signed by OzoneManager
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws OMException {
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new OMException("Delegation Token can be issued only with "
            + "kerberos or web authentication",
            INVALID_AUTH_METHOD);
      }
      if (delegationTokenMgr == null || !delegationTokenMgr.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running in OM.");
        return null;
      }

      UserGroupInformation ugi = getRemoteUser();
      String user = ugi.getUserName();
      Text owner = new Text(user);
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }

      return delegationTokenMgr.createToken(owner, renewer, realUser);
    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      LOG.error("Get Delegation token failed, cause: {}", ex.getMessage());
      throw new OMException("Get Delegation token failed.", ex,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Method to renew a delegationToken issued by OzoneManager.
   *
   * @param token token to renew
   * @return new expiryTime of the token
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    long expiryTime;

    try {

      if (!isAllowedDelegationTokenOp()) {
        throw new OMException("Delegation Token can be renewed only with "
            + "kerberos or web authentication",
            INVALID_AUTH_METHOD);
      }
      String renewer = getRemoteUser().getShortUserName();
      expiryTime = delegationTokenMgr.renewToken(token, renewer);

    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      OzoneTokenIdentifier id = null;
      try {
        id = OzoneTokenIdentifier.readProtoBuf(token.getIdentifier());
      } catch (IOException ignored) {
      }
      LOG.error("Delegation token renewal failed for dt id: {}, cause: {}",
          id, ex.getMessage());
      throw new OMException("Delegation token renewal failed for dt: " + token,
          ex, TOKEN_ERROR_OTHER);
    }
    return expiryTime;
  }

  /**
   * Cancels a delegation token.
   *
   * @param token token to cancel
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    try {
      String canceller = getRemoteUser().getUserName();
      final OzoneTokenIdentifier id = delegationTokenMgr.cancelToken(token, canceller);
      LOG.trace("Delegation token cancelled for dt: {}", id);
    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      LOG.error("Delegation token cancellation failed for dt id: {}, cause: {}",
          token.getIdentifier(), ex.getMessage());
      throw new OMException("Delegation token renewal failed for dt: " + token,
          ex, TOKEN_ERROR_OTHER);
    }
  }

  public boolean isOwner(UserGroupInformation callerUgi, String ownerName) {
    if (ownerName == null) {
      return false;
    }
    return callerUgi.getUserName().equals(ownerName) ||
        callerUgi.getShortUserName().equals(ownerName);
  }

  /**
   * A variant of checkAcls that doesn't throw exception if permission denied.
   *
   * @return true if permission granted, false if permission denied.
   */
  private boolean hasAcls(String userName, ResourceType resType,
      StoreType store, ACLType acl, String vol, String bucket, String key) {
    try {
      return checkAcls(resType, store, acl, vol, bucket, key,
          UserGroupInformation.createRemoteUser(userName),
          ProtobufRpcEngine.Server.getRemoteIp(),
          ProtobufRpcEngine.Server.getRemoteIp().getHostName(),
          false, getVolumeOwner(vol, acl, resType));
    } catch (OMException ex) {
      // Should not trigger exception here at all
      return false;
    }
  }

  public String getVolumeOwner(String vol, ACLType type, ResourceType resType)
      throws OMException {
    String volOwnerName = null;
    if (!vol.equals(OzoneConsts.OZONE_ROOT) &&
        !(type == ACLType.CREATE && resType == ResourceType.VOLUME)) {
      volOwnerName = getVolumeOwner(vol);
    }
    return volOwnerName;
  }

  private String getVolumeOwner(String volume) throws OMException {
    OMLockDetails omLockDetails = metadataManager.getLock().acquireReadLock(
        VOLUME_LOCK, volume);
    boolean lockAcquired = omLockDetails.isLockAcquired();
    String dbVolumeKey = metadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs;
    try {
      volumeArgs = metadataManager.getVolumeTable().get(dbVolumeKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getVolumeOwner for Volume " + volume + " failed",
            ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
      }
    }
    if (volumeArgs != null) {
      return volumeArgs.getOwnerName();
    } else {
      throw new OMException("Volume " + volume + " is not found",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }
  }

  /**
   * Return the owner of a given bucket.
   *
   * @return String
   */
  public String getBucketOwner(String volume, String bucket, ACLType type,
       ResourceType resType) throws OMException {
    String bucketOwner = null;
    if ((resType != ResourceType.VOLUME) &&
        !(type == ACLType.CREATE && resType == ResourceType.BUCKET)) {
      bucketOwner = getBucketOwner(volume, bucket);
    }
    return bucketOwner;
  }

  private String getBucketOwner(String volume, String bucket)
      throws OMException {
    OmBucketInfo bucketInfo;

    OMLockDetails omLockDetails = metadataManager.getLock().acquireReadLock(
        BUCKET_LOCK, volume, bucket);
    boolean lockAcquired = omLockDetails.isLockAcquired();

    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      bucketInfo = metadataManager.getBucketTable().get(dbBucketKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getBucketOwner for Bucket " + volume + "/" +
            bucket  + " failed: " + ioe.getMessage(),
            ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
      }
    }
    if (bucketInfo != null) {
      return bucketInfo.getOwner();
    } else {
      throw new OMException("Bucket not found", ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  @SuppressWarnings("parameternumber")
  public boolean checkAcls(ResourceType resType, StoreType storeType,
      ACLType aclType, String vol, String bucket, String key,
      UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
      boolean throwIfPermissionDenied, String owner)
      throws OMException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(storeType)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(ACLIdentityType.USER)
        .setAclRights(aclType)
        .setOwnerName(owner)
        .build();

    return omMetadataReader.checkAcls(obj, context, throwIfPermissionDenied);
  }



  /**
   * Return true if Ozone acl's are enabled, else false.
   *
   * @return boolean
   */
  public boolean getAclsEnabled() {
    return isAclEnabled;
  }

  public boolean getAllowListAllVolumes() {
    return allowListAllVolumes;
  }

  public ReferenceCounted<IOmMetadataReader> getOmMetadataReader() {
    return rcOmMetadataReader;
  }

  /**
   * Return true if SPNEGO auth is enabled for OM HTTP server, otherwise false.
   *
   * @return boolean
   */
  public boolean isSpnegoEnabled() {
    return isSpnegoEnabled;
  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    try {
      if (isAclEnabled) {
        omMetadataReader.checkAcls(ResourceType.VOLUME,
            StoreType.OZONE, ACLType.READ, volume,
            null, null);
      }
      metrics.incNumVolumeInfos();
      return volumeManager.getVolumeInfo(volume);
    } catch (Exception ex) {
      metrics.incNumVolumeInfoFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_VOLUME,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_VOLUME,
            auditMap));
      }
    }
  }

  /**
   * Lists volumes accessible by a specific user.
   *
   * @param userName - user name
   * @param prefix   - Filter prefix -- Return only entries that match this.
   * @param prevKey  - Previous key -- List starts from the next from the
   *                 prevkey
   * @param maxKeys  - Max number of keys to return.
   * @return List of Volumes.
   */
  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix,
      String prevKey, int maxKeys) throws IOException {
    UserGroupInformation remoteUserUgi =
        ProtobufRpcEngine.Server.getRemoteUser();
    if (isAclEnabled) {
      if (remoteUserUgi == null) {
        LOG.error("Rpc user UGI is null. Authorization failed.");
        throw new OMException("Rpc user UGI is null. Authorization failed.",
            ResultCodes.PERMISSION_DENIED);
      }
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.PREV_KEY, prevKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.USERNAME, userName);
    try {
      metrics.incNumVolumeLists();
      if (isAclEnabled) {
        String remoteUserName = remoteUserUgi.getShortUserName();
        // if not admin nor list my own volumes, check ACL.
        if (!remoteUserName.equals(userName) && !isAdmin(remoteUserUgi)) {
          omMetadataReader.checkAcls(ResourceType.VOLUME,
              StoreType.OZONE, ACLType.LIST,
              OzoneConsts.OZONE_ROOT, null, null);
        }
        // List all volumes first
        List<OmVolumeArgs> listAllVolumes = volumeManager.listVolumes(
            null, prefix, prevKey, maxKeys);
        List<OmVolumeArgs> result = new ArrayList<>();
        // Filter all volumes by LIST ACL
        for (OmVolumeArgs volumeArgs : listAllVolumes) {
          if (hasAcls(userName, ResourceType.VOLUME, StoreType.OZONE,
              ACLType.LIST, volumeArgs.getVolume(), null, null)) {
            result.add(volumeArgs);
          }
        }
        return result;
      } else {
        // When ACL is not enabled, fallback to filter by owner
        return volumeManager.listVolumes(userName, prefix, prevKey, maxKeys);
      }
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_VOLUMES,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_VOLUMES,
            auditMap));
      }
    }
  }

  /**
   * Lists volume all volumes in the cluster.
   *
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   *                prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   */
  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey, int
      maxKeys) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.PREV_KEY, prevKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.USERNAME, null);
    try {
      metrics.incNumVolumeLists();
      if (isAclEnabled) {
        omMetadataReader.checkAcls(ResourceType.VOLUME,
            StoreType.OZONE, ACLType.LIST,
            OzoneConsts.OZONE_ROOT, null, null);
      }
      return volumeManager.listVolumes(null, prefix, prevKey, maxKeys);
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_VOLUMES,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_VOLUMES,
            auditMap));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBuckets(String volumeName, String startKey,
                                        String prefix, int maxNumOfBuckets,
                                        boolean hasSnapshot)
      throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_NUM_OF_BUCKETS,
        String.valueOf(maxNumOfBuckets));
    auditMap.put(OzoneConsts.HAS_SNAPSHOT, String.valueOf(hasSnapshot));

    try {
      if (isAclEnabled) {
        omMetadataReader.checkAcls(ResourceType.VOLUME,
            StoreType.OZONE, ACLType.LIST,
            volumeName, null, null);
      }
      metrics.incNumBucketLists();
      return bucketManager.listBuckets(volumeName,
          startKey, prefix, maxNumOfBuckets, hasSnapshot);
    } catch (IOException ex) {
      metrics.incNumBucketListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_BUCKETS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_BUCKETS,
            auditMap));
      }
    }
  }

  /**
   * Gets the bucket information.
   *
   * @param volume - Volume name.
   * @param bucket - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   */
  @Override
  public OmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.BUCKET, bucket);
    try {
      if (isAclEnabled) {
        omMetadataReader.checkAcls(ResourceType.BUCKET,
            StoreType.OZONE, ACLType.READ, volume,
            bucket, null);
      }
      metrics.incNumBucketInfos();

      OmBucketInfo bucketInfo = bucketManager.getBucketInfo(volume, bucket);

      // No links - return the bucket info right away.
      if (!bucketInfo.isLink()) {
        return bucketInfo;
      }
      // Otherwise follow the links to find the real bucket.
      // We already know that `bucketInfo` is a linked one,
      // so we skip one `getBucketInfo` and start with the known link.
      ResolvedBucket resolvedBucket =
          resolveBucketLink(Pair.of(
                  bucketInfo.getSourceVolume(),
                  bucketInfo.getSourceBucket()),
              true);

      // If it is a dangling link it means no real bucket exists,
      // for example, it could have been deleted, but the links still present.
      if (!resolvedBucket.isDangling()) {
        OmBucketInfo realBucket =
            bucketManager.getBucketInfo(
                resolvedBucket.realVolume(),
                resolvedBucket.realBucket());
        // Pass the real bucket metadata in the link bucket info.
        return bucketInfo.toBuilder()
            .setDefaultReplicationConfig(
                realBucket.getDefaultReplicationConfig())
            .setIsVersionEnabled(realBucket.getIsVersionEnabled())
            .setStorageType(realBucket.getStorageType())
            .setQuotaInBytes(realBucket.getQuotaInBytes())
            .setQuotaInNamespace(realBucket.getQuotaInNamespace())
            .setUsedBytes(realBucket.getUsedBytes())
            .setUsedNamespace(realBucket.getUsedNamespace())
            .addAllMetadata(realBucket.getMetadata())
            .setBucketLayout(realBucket.getBucketLayout())
            .build();
      }
      // If no real bucket exists, return the requested one's info.
      return bucketInfo;
    } catch (Exception ex) {
      metrics.incNumBucketInfoFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_BUCKET,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_BUCKET,
            auditMap));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader = getReader(args)) {
      return rcReader.get().lookupKey(args);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KeyInfoWithVolumeContext getKeyInfo(final OmKeyArgs args,
                                             boolean assumeS3Context)
      throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader = getReader(args)) {
      return rcReader.get().getKeyInfo(args, assumeS3Context);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListKeysResult listKeys(String volumeName, String bucketName,
                                 String startKey, String keyPrefix, int maxKeys)
      throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader =
             getReader(volumeName, bucketName, keyPrefix)) {
      return rcReader.get().listKeys(
          volumeName, bucketName, startKey, keyPrefix, maxKeys);
    }
  }

  @Override
  public ListKeysLightResult listKeysLight(String volumeName,
                                           String bucketName,
                                           String startKey, String keyPrefix,
                                           int maxKeys) throws IOException {
    ListKeysResult listKeysResult =
        listKeys(volumeName, bucketName, startKey, keyPrefix, maxKeys);
    List<OmKeyInfo> keys = listKeysResult.getKeys();
    List<BasicOmKeyInfo> basicKeysList =
        keys.stream().map(BasicOmKeyInfo::fromOmKeyInfo)
            .collect(Collectors.toList());

    return new ListKeysLightResult(basicKeysList, listKeysResult.isTruncated());
  }

  @Override
  public SnapshotInfo getSnapshotInfo(String volumeName, String bucketName,
                                      String snapshotName) throws IOException {
    metrics.incNumSnapshotInfos();
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    try {
      // Updating the volumeName & bucketName in case the bucket is a linked bucket. We need to do this before a
      // permission check, since linked bucket permissions and source bucket permissions could be different.
      ResolvedBucket resolvedBucket = resolveBucketLink(Pair.of(volumeName, bucketName));
      auditMap = buildAuditMap(resolvedBucket.realVolume());
      auditMap.put(OzoneConsts.BUCKET, resolvedBucket.realBucket());
      SnapshotInfo snapshotInfo =
          metadataManager.getSnapshotInfo(resolvedBucket.realVolume(), resolvedBucket.realBucket(), snapshotName);

      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          OMAction.SNAPSHOT_INFO, auditMap));
      return snapshotInfo;
    } catch (Exception ex) {
      metrics.incNumSnapshotInfoFails();
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.SNAPSHOT_INFO,
          auditMap, ex));
      throw ex;
    }
  }

  @Override
  public ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException {
    metrics.incNumSnapshotLists();
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    try {
      // Updating the volumeName & bucketName in case the bucket is a linked bucket. We need to do this before a
      // permission check, since linked bucket permissions and source bucket permissions could be different.
      ResolvedBucket resolvedBucket = resolveBucketLink(Pair.of(volumeName, bucketName));
      auditMap = buildAuditMap(resolvedBucket.realVolume());
      auditMap.put(OzoneConsts.BUCKET, resolvedBucket.realBucket());
      if (isAclEnabled) {
        omMetadataReader.checkAcls(ResourceType.BUCKET, StoreType.OZONE,
            ACLType.LIST, resolvedBucket.realVolume(), resolvedBucket.realBucket(), null);
      }
      ListSnapshotResponse listSnapshotResponse =
          metadataManager.listSnapshot(resolvedBucket.realVolume(), resolvedBucket.realBucket(),
              snapshotPrefix, prevSnapshot, maxListResult);

      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          OMAction.LIST_SNAPSHOT, auditMap));
      return listSnapshotResponse;
    } catch (Exception ex) {
      metrics.incNumSnapshotListFails();
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_SNAPSHOT,
          auditMap, ex));
      throw ex;
    }
  }

  /**
   * Gets the lifecycle configuration information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return OmLifecycleConfiguration or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmLifecycleConfiguration getLifecycleConfiguration(String volumeName,
      String bucketName) throws IOException {
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    ResolvedBucket resolvedBucket = resolveBucketLink(Pair.of(volumeName, bucketName));
    auditMap = buildAuditMap(resolvedBucket.realVolume());
    auditMap.put(OzoneConsts.BUCKET, resolvedBucket.realBucket());

    if (isAclEnabled) {
      omMetadataReader.checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.READ,
          resolvedBucket.realVolume(), resolvedBucket.realBucket(), null);
    }

    boolean auditSuccess = true;
    OMLockDetails omLockDetails = metadataManager.getLock().acquireReadLock(BUCKET_LOCK,
        resolvedBucket.realVolume(), resolvedBucket.realBucket());
    boolean lockAcquired = omLockDetails.isLockAcquired();
    try {
      return metadataManager.getLifecycleConfiguration(
          resolvedBucket.realVolume(), resolvedBucket.realBucket());
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          OMAction.GET_LIFECYCLE_CONFIGURATION, auditMap, ex));
      throw ex;
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(BUCKET_LOCK,
            resolvedBucket.realVolume(), resolvedBucket.realBucket());
      }
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.GET_LIFECYCLE_CONFIGURATION, auditMap));
      }
    }
  }

  @Override
  public GetLifecycleServiceStatusResponse getLifecycleServiceStatus() {
    KeyLifecycleService keyLifecycleService = keyManager.getKeyLifecycleService();
    if (keyLifecycleService == null) {
      return GetLifecycleServiceStatusResponse.newBuilder()
          .setIsEnabled(getConfiguration().getBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED,
              OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT))
          .build();
    }
    return keyLifecycleService.status();
  }

  private Map<String, String> buildAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }

  public AuditLogger getAuditLogger() {
    return AUDIT;
  }

  public AuditLogger getSystemAuditLogger() {
    return SYSTEMAUDIT;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {
    return omMetadataReader.buildAuditMessageForSuccess(op, auditMap);
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {
    return omMetadataReader.buildAuditMessageForFailure(op,
        auditMap, throwable);
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.omInfoBeanName = HddsUtils.registerWithJmxProperties(
        "OzoneManager", "OzoneManagerInfo", jmxProperties, this);
  }

  private void unregisterMXBean() {
    if (this.omInfoBeanName != null) {
      MBeans.unregister(this.omInfoBeanName);
      this.omInfoBeanName = null;
    }
  }

  @Override
  public String getNamespace() {
    return omNodeDetails.getServiceId();
  }

  @Override
  public String getRpcPort() {
    return String.valueOf(omRpcAddress.getPort());
  }

  private static List<List<String>> getRatisRolesException(String exceptionString) {
    return Collections.singletonList(Collections.singletonList(exceptionString));
  }

  @Override
  public List<List<String>> getRatisRoles() {
    int port = omNodeDetails.getRatisPort();
    if (null == omRatisServer) {
      return getRatisRolesException("Server is shutting down");
    }
    String leaderReadiness = omRatisServer.getLeaderStatus().name();
    final RaftPeerId leaderId = omRatisServer.getLeaderId();
    if (leaderId == null) {
      LOG.error(NO_LEADER_ERROR_MESSAGE);
      return getRatisRolesException(NO_LEADER_ERROR_MESSAGE);
    }

    final List<ServiceInfo> serviceList;
    try {
      serviceList = getServiceList();
    } catch (IOException e) {
      LOG.error("Failed to getServiceList", e);
      return getRatisRolesException("IO-Exception Occurred, " + e.getMessage());
    }
    return OmUtils.format(serviceList, port, leaderId.toString(), leaderReadiness);
  }

  /**
   * Create OMHAMetrics instance.
   */
  public void omHAMetricsInit(String leaderId) {
    // unregister, in case metrics already exist
    // so that the metrics will get updated.
    OMHAMetrics.unRegister();
    omhaMetrics = OMHAMetrics
        .create(getOMNodeId(), leaderId);
  }

  @VisibleForTesting
  public OMHAMetrics getOmhaMetrics() {
    return omhaMetrics;
  }

  @Override
  public String getRatisLogDirectory() {
    return  OzoneManagerRatisUtils.getOMRatisDirectory(configuration);
  }

  @Override
  public String getRocksDbDirectory() {
    return String.valueOf(OMStorage.getOmDbDir(configuration));
  }

  @Override
  public String getHostname() {
    return omHostName;
  }

  @VisibleForTesting
  public OzoneManagerHttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public List<ServiceInfo> getServiceList() throws IOException {
    // When we implement multi-home this call has to be handled properly.
    List<ServiceInfo> services = new ArrayList<>();
    ServiceInfo.Builder omServiceInfoBuilder = ServiceInfo.newBuilder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname(omRpcAddress.getHostName())
        .setOmVersion(OzoneManagerVersion.CURRENT)
        .addServicePort(ServicePort.newBuilder()
            .setType(ServicePort.Type.RPC)
            .setValue(omRpcAddress.getPort())
            .build());
    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(configuration);
    URI keyProviderUri = KMSUtil.getKeyProviderUri(
        hadoopConfig,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
    String keyProviderUriStr =
        (keyProviderUri != null) ? keyProviderUri.toString() : null;
    omServiceInfoBuilder.setServerDefaults(
        new OzoneFsServerDefaults(keyProviderUriStr));
    if (httpServer != null
        && httpServer.getHttpAddress() != null) {
      omServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
          .setType(ServicePort.Type.HTTP)
          .setValue(httpServer.getHttpAddress().getPort())
          .build());
    }
    if (httpServer != null
        && httpServer.getHttpsAddress() != null) {
      omServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
          .setType(ServicePort.Type.HTTPS)
          .setValue(httpServer.getHttpsAddress().getPort())
          .build());
    }

    // Since this OM is processing the request, we can assume it to be the
    // leader OM

    OMRoleInfo omRole = OMRoleInfo.newBuilder()
        .setNodeId(getOMNodeId())
        .setServerRole(RaftPeerRole.LEADER.name())
        .build();
    omServiceInfoBuilder.setOmRoleInfo(omRole);

    if (omRatisServer != null) {
      omServiceInfoBuilder.addServicePort(ServicePort.newBuilder()
          .setType(ServicePort.Type.RATIS)
          .setValue(omNodeDetails.getRatisPort())
          .build());
    }

    for (OMNodeDetails peerNode : peerNodesMap.values()) {
      ServiceInfo.Builder peerOmServiceInfoBuilder = ServiceInfo.newBuilder()
          .setNodeType(HddsProtos.NodeType.OM)
          .setHostname(peerNode.getHostName())
          // For now assume peer is at the same version.
          // This field needs to be fetched from peer when rolling upgrades
          // are implemented.
          .setOmVersion(OzoneManagerVersion.CURRENT)
          .addServicePort(ServicePort.newBuilder()
              .setType(ServicePort.Type.RPC)
              .setValue(peerNode.getRpcPort())
              .build());

      OMRoleInfo peerOmRole = OMRoleInfo.newBuilder()
          .setNodeId(peerNode.getNodeId())
          .setServerRole(RaftPeerRole.FOLLOWER.name())
          .build();
      peerOmServiceInfoBuilder.setOmRoleInfo(peerOmRole);

      services.add(peerOmServiceInfoBuilder.build());
    }

    services.add(omServiceInfoBuilder.build());

    // For client we have to return SCM with container protocol port,
    // not block protocol. This is information is being not used by
    // RpcClient, but for compatibility leaving as it is and also making sure
    // that this works for SCM HA.
    Collection<InetSocketAddress> scmAddresses = getScmAddressForClients(
        configuration);

    for (InetSocketAddress scmAddr : scmAddresses) {
      ServiceInfo.Builder scmServiceInfoBuilder = ServiceInfo.newBuilder()
          .setNodeType(HddsProtos.NodeType.SCM)
          .setHostname(scmAddr.getHostName())
          .addServicePort(ServicePort.newBuilder()
              .setType(ServicePort.Type.RPC)
              .setValue(scmAddr.getPort()).build());
      services.add(scmServiceInfoBuilder.build());
    }
    metrics.incNumGetServiceLists();
    // For now there is no exception that can can happen in this call,
    // so failure metrics is not handled. In future if there is any need to
    // handle exception in this method, we need to incorporate
    // metrics.incNumGetServiceListFails()
    AUDIT.logReadSuccess(
        buildAuditMessageForSuccess(OMAction.GET_SERVICE_LIST,
            new LinkedHashMap<>()));
    return services;
  }

  @Override
  public ServiceInfoEx getServiceInfo() throws IOException {
    return serviceInfo.provide();
  }

  @Override
  public ListOpenFilesResult listOpenFiles(String path,
                                           int maxKeys,
                                           String contToken)
      throws IOException {

    metrics.incNumListOpenFiles();
    checkAdminUserPrivilege("list open files.");

    // Using final to make sure they are assigned once and only once in
    // every branch.
    final String dbOpenKeyPrefix, dbContTokenPrefix;
    final String volumeName, bucketName;
    final BucketLayout bucketLayout;

    // Process path prefix
    if (path == null || path.isEmpty() || path.equals(OM_KEY_PREFIX)) {
      // path is root
      dbOpenKeyPrefix = "";
      volumeName = "";
      bucketName = "";
      // default to FSO's OpenFileTable. TODO: client option to pass OBS/LEGACY?
      bucketLayout = BucketLayout.FILE_SYSTEM_OPTIMIZED;
    } else {
      // path is bucket or key prefix, break it down to volume, bucket, prefix
      StringTokenizer tokenizer = new StringTokenizer(path, OM_KEY_PREFIX);
      // Validate path to avoid NoSuchElementException
      if (tokenizer.countTokens() < 2) {
        metrics.incNumListOpenFilesFails();
        throw new OMException("Invalid path: " + path + ". " +
            "Only root level or bucket level path is supported at this time",
            INVALID_PATH);
      }

      volumeName = tokenizer.nextToken();
      bucketName = tokenizer.nextToken();

      OmBucketInfo bucketInfo;
      try {
        // as expected, getBucketInfo throws if volume or bucket does not exist
        bucketInfo = getBucketInfo(volumeName, bucketName);
      } catch (OMException ex) {
        metrics.incNumListOpenFilesFails();
        throw ex;
      } catch (IOException ex) {
        // Wrap IOException in OMException
        metrics.incNumListOpenFilesFails();
        throw new OMException(ex.getMessage(), NOT_SUPPORTED_OPERATION);
      }

      final String keyPrefix;
      if (tokenizer.hasMoreTokens()) {
        // Collect the rest but trim the leading "/"
        keyPrefix = tokenizer.nextToken("").substring(1);
      } else {
        keyPrefix = "";
      }

      // Determine dbKey prefix based on the bucket type
      bucketLayout = bucketInfo.getBucketLayout();
      switch (bucketLayout) {
      case FILE_SYSTEM_OPTIMIZED:
        dbOpenKeyPrefix = metadataManager.getOzoneKeyFSO(
            volumeName, bucketName, keyPrefix);
        break;
      case OBJECT_STORE:
      case LEGACY:
        dbOpenKeyPrefix = metadataManager.getOzoneKey(
            volumeName, bucketName, keyPrefix);
        break;
      default:
        metrics.incNumListOpenFilesFails();
        throw new OMException("Unsupported bucket layout: " +
            bucketInfo.getBucketLayout(), NOT_SUPPORTED_OPERATION);
      }
    }

    // Process cont. token
    if (contToken == null || contToken.isEmpty()) {
      // if a continuation token is not specified
      dbContTokenPrefix = dbOpenKeyPrefix;
    } else {
      dbContTokenPrefix = contToken;
    }

    // arg processing done. call inner impl (table iteration)
    return metadataManager.listOpenFiles(
        bucketLayout, maxKeys, dbOpenKeyPrefix,
        !StringUtils.isEmpty(contToken), dbContTokenPrefix);
  }

  @Override
  public void transferLeadership(String newLeaderId)
      throws IOException {
    checkAdminUserPrivilege("transfer raft leadership.");
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("newLeaderId", newLeaderId);
    try {
      final RaftServer.Division division = omRatisServer.getServerDivision();
      final RaftPeerId targetPeerId;
      if (newLeaderId.isEmpty()) {
        final RaftPeerId curLeader = omRatisServer.getLeaderId();
        targetPeerId = division.getGroup().getPeers().stream()
            .map(RaftPeer::getId)
            .filter(id -> !id.equals(curLeader))
            .findFirst()
            .orElseThrow(() -> new IOException("Cannot find a new leader to transfer leadership."));
      } else {
        targetPeerId = RaftPeerId.valueOf(newLeaderId);
      }

      final GrpcTlsConfig tlsConfig =
          OzoneManagerRatisUtils.createServerTlsConfig(secConfig, certClient);

      RatisHelper.transferRatisLeadership(configuration, division.getGroup(),
          targetPeerId, tlsConfig);
    } catch (IOException ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(OMAction.TRANSFER_LEADERSHIP,
              auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.TRANSFER_LEADERSHIP,
                auditMap));
      }
    }
  }

  @Override
  public boolean triggerRangerBGSync(boolean noWait) throws IOException {

    // OM should be leader and ready.
    // This check should always pass if called from a client since there
    // is a leader check somewhere before entering this method.
    if (!isLeaderReady()) {
      // And even if we could allow followers to trigger sync, checkLeader()
      // calls inside the sync would quit the sync anyway.
      throw new OMException("OM is not leader or not ready", INVALID_REQUEST);
    }

    final UserGroupInformation ugi = getRemoteUser();
    // Check Ozone admin privilege
    if (!isAdmin(ugi)) {
      throw new OMException("Only Ozone admins are allowed to trigger "
          + "Ranger background sync manually", PERMISSION_DENIED);
    }

    // Check if MT manager is inited
    final OMMultiTenantManager mtManager = getMultiTenantManager();
    if (mtManager == null) {
      throw new OMException("S3 Multi-Tenancy is not enabled",
          FEATURE_NOT_ENABLED);
    }

    // Check if Ranger BG sync task is inited
    final OMRangerBGSyncService bgSync = mtManager.getOMRangerBGSyncService();
    if (bgSync == null) {
      throw new OMException("Ranger background sync service is not initialized",
          FEATURE_NOT_ENABLED);
    }

    // Trigger Ranger BG Sync
    if (noWait) {
      final Thread t = new Thread(bgSync::triggerRangerSyncOnce,
          threadPrefix + "RangerSync");
      t.start();
      LOG.info("User '{}' manually triggered Multi-Tenancy Ranger Sync "
          + "in a new thread, tid={}", ugi, t.getId());
      return true;
    } else {
      LOG.info("User '{}' manually triggered Multi-Tenancy Ranger Sync", ugi);
      // Block in the handler thread
      return bgSync.triggerRangerSyncOnce();
    }
  }

  @Override
  public StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException {
    return upgradeFinalizer.finalize(upgradeClientID, this);
  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException {
    if (readonly) {
      return new StatusAndMessages(upgradeFinalizer.getStatus(),
          Collections.emptyList());
    }
    return upgradeFinalizer.reportStatus(upgradeClientID, takeover);
  }

  /**
   * List tenants.
   */
  @Override
  public TenantStateList listTenant() throws IOException {

    metrics.incNumTenantLists();

    final UserGroupInformation ugi = getRemoteUser();
    if (!isAdmin(ugi)) {
      final OMException omEx = new OMException(
          "Only Ozone admins are allowed to list tenants.", PERMISSION_DENIED);
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          OMAction.LIST_TENANT, new LinkedHashMap<>(), omEx));
      throw omEx;
    }

    final Table<String, OmDBTenantState> tenantStateTable =
        metadataManager.getTenantStateTable();

    // Won't iterate cache here, mainly because we can't acquire a read lock
    // for cache iteration: no tenant is specified, hence no volume name to
    // acquire VOLUME_LOCK on. There could be a few millis delay before entries
    // are flushed to the table. This should be acceptable for a list tenant
    // request.

    try (TableIterator<String, ? extends KeyValue<String, OmDBTenantState>>
        iterator = tenantStateTable.iterator()) {

      final List<TenantState> tenantStateList = new ArrayList<>();

      // Iterate table
      while (iterator.hasNext()) {
        final Table.KeyValue<String, OmDBTenantState> dbEntry = iterator.next();
        final String tenantId = dbEntry.getKey();
        final OmDBTenantState omDBTenantState = dbEntry.getValue();
        assert (tenantId.equals(omDBTenantState.getTenantId()));
        tenantStateList.add(TenantState.newBuilder()
            .setTenantId(omDBTenantState.getTenantId())
            .setBucketNamespaceName(omDBTenantState.getBucketNamespaceName())
            .setUserRoleName(omDBTenantState.getUserRoleName())
            .setAdminRoleName(omDBTenantState.getAdminRoleName())
            .setBucketNamespacePolicyName(
                omDBTenantState.getBucketNamespacePolicyName())
            .setBucketPolicyName(omDBTenantState.getBucketPolicyName())
            .build());
      }

      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          OMAction.LIST_TENANT, new LinkedHashMap<>()));

      return new TenantStateList(tenantStateList);
    }
  }

  /**
   * Tenant get user info.
   */
  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException {

    metrics.incNumTenantGetUserInfos();

    if (StringUtils.isEmpty(userPrincipal)) {
      return null;
    }

    final List<ExtendedUserAccessIdInfo> accessIdInfoList = new ArrayList<>();

    // Won't iterate cache here for a similar reason as in OM#listTenant
    //  tenantGetUserInfo lists all accessIds assigned to a user across
    //  multiple tenants.

    // Retrieve the list of accessIds associated to this user principal
    final OmDBUserPrincipalInfo kerberosPrincipalInfo =
        metadataManager.getPrincipalToAccessIdsTable().get(userPrincipal);
    if (kerberosPrincipalInfo == null) {
      return null;
    }
    final Set<String> accessIds = kerberosPrincipalInfo.getAccessIds();

    final Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("userPrincipal", userPrincipal);

    accessIds.forEach(accessId -> {
      try {
        final OmDBAccessIdInfo accessIdInfo =
            metadataManager.getTenantAccessIdTable().get(accessId);
        if (accessIdInfo == null) {
          // As we are not acquiring a lock, the accessId entry might have been
          //  removed from the TenantAccessIdTable already.
          //  Log a warning (shouldn't happen very often) and move on.
          LOG.warn("Expected accessId '{}' not found in TenantAccessIdTable. "
                  + "Might have been removed already.", accessId);
          return;
        }
        assert (accessIdInfo.getUserPrincipal().equals(userPrincipal));
        accessIdInfoList.add(ExtendedUserAccessIdInfo.newBuilder()
            .setUserPrincipal(userPrincipal)
            .setAccessId(accessId)
            .setTenantId(accessIdInfo.getTenantId())
            .setIsAdmin(accessIdInfo.getIsAdmin())
            .setIsDelegatedAdmin(accessIdInfo.getIsDelegatedAdmin())
            .build());
      } catch (IOException e) {
        LOG.error("Potential DB issue. Failed to retrieve OmDBAccessIdInfo "
            + "for accessId '{}' in TenantAccessIdTable.", accessId);
        // Audit
        auditMap.put("accessId", accessId);
        AUDIT.logWriteFailure(buildAuditMessageForFailure(
            OMAction.TENANT_GET_USER_INFO, auditMap, e));
        auditMap.remove("accessId");
      }
    });

    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        OMAction.TENANT_GET_USER_INFO, auditMap));

    return new TenantUserInfoValue(accessIdInfoList);
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException {

    metrics.incNumTenantUserLists();

    if (StringUtils.isEmpty(tenantId)) {
      return null;
    }

    multiTenantManager.checkTenantExistence(tenantId);

    final String volumeName = multiTenantManager.getTenantVolumeName(tenantId);
    final Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.TENANT, tenantId);
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.USER_PREFIX, prefix);

    OMLockDetails omLockDetails =
        metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName);
    boolean lockAcquired = omLockDetails.isLockAcquired();
    try {
      final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
      if (!multiTenantManager.isTenantAdmin(ugi, tenantId, false)) {
        throw new OMException("Only tenant and ozone admins can access this " +
            "API. '" + ugi.getShortUserName() + "' is not an admin.",
            PERMISSION_DENIED);
      }
      final TenantUserList userList =
          multiTenantManager.listUsersInTenant(tenantId, prefix);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          OMAction.TENANT_LIST_USER, auditMap));
      return userList;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          OMAction.TENANT_LIST_USER, auditMap, ex));
      throw ex;
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volumeName);
      }
    }
  }

  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {
    return getS3VolumeContext(false);
  }

  S3VolumeContext getS3VolumeContext(boolean skipChecks) throws IOException {
    long start = Time.monotonicNowNanos();
    // Unless the OM request contains S3 authentication info with an access
    // ID that corresponds to a tenant volume, the request will be directed
    // to the default S3 volume.
    String s3Volume = HddsClientUtils.getDefaultS3VolumeName(configuration);
    S3Authentication s3Auth = getS3Auth();
    final String userPrincipal;

    if (s3Auth == null) {
      // This is the default user principal if request does not have S3Auth set
      userPrincipal = Server.getRemoteUser().getShortUserName();

      if (LOG.isDebugEnabled()) {
        // An old S3 gateway talking to a new OM may not attach the auth info.
        // This old version of s3g will also not have a client that supports
        // multi-tenancy, so we can direct requests to the default S3 volume.
        LOG.debug("S3 authentication was not attached to the OM request. " +
                "Directing requests to the default S3 volume {}.",
            s3Volume);
      }
    } else {
      String accessId = s3Auth.getAccessId();
      // If S3 Multi-Tenancy is not enabled, all S3 requests will be redirected
      // to the default s3v for compatibility
      final Optional<String> optionalTenantId = isS3MultiTenancyEnabled() ?
          multiTenantManager.getTenantForAccessID(accessId) : Optional.empty();

      if (!optionalTenantId.isPresent()) {
        final UserGroupInformation s3gUGI =
            UserGroupInformation.createRemoteUser(accessId);
        // When the accessId belongs to the default s3v (i.e. when the accessId
        // key pair is generated using the regular `ozone s3 getsecret`), the
        // user principal returned here should simply be the accessId's short
        // user name (processed by the auth_to_local rule)
        userPrincipal = s3gUGI.getShortUserName();

        if (LOG.isDebugEnabled()) {
          LOG.debug("No tenant found for access ID {}. Directing "
              + "requests to default s3 volume {}.", accessId, s3Volume);
        }
      } else {
        // S3 Multi-Tenancy is enabled, and the accessId is assigned to a tenant

        final String tenantId = optionalTenantId.get();

        OmDBTenantState tenantState =
            metadataManager.getTenantStateTable().get(tenantId);
        if (tenantState != null) {
          s3Volume = tenantState.getBucketNamespaceName();
        } else {
          String message = "Unable to find tenant '" + tenantId
              + "' details for access ID " + accessId
              + ". The tenant might have been removed during this operation, "
              + "or the OM DB is inconsistent";
          LOG.warn(message);
          throw new OMException(message, ResultCodes.TENANT_NOT_FOUND);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Get S3 volume request for access ID {} belonging to " +
                  "tenant {} is directed to the volume {}.", accessId, tenantId,
              s3Volume);
        }

        OMLockDetails omLockDetails = getMetadataManager().getLock()
            .acquireReadLock(VOLUME_LOCK, s3Volume);
        boolean acquiredVolumeLock = omLockDetails.isLockAcquired();

        try {
          // Inject user name to the response to be used for KMS on the client
          userPrincipal = OzoneAclUtils.accessIdToUserPrincipal(accessId);
        } finally {
          if (acquiredVolumeLock) {
            getMetadataManager().getLock().releaseReadLock(
                VOLUME_LOCK, s3Volume);
          }
        }
      }
    }


    final OmVolumeArgs volumeInfo;
    if (skipChecks) {
      // for internal usages, skip acl checks and metrics.
      volumeInfo = volumeManager.getVolumeInfo(s3Volume);
    } else {
      // for external usages, getVolumeInfo() performs acl checks
      // and metric updates.
      volumeInfo = getVolumeInfo(s3Volume);
    }

    final S3VolumeContext.Builder s3VolumeContext = S3VolumeContext.newBuilder()
        .setOmVolumeArgs(volumeInfo)
        .setUserPrincipal(userPrincipal);
    perfMetrics.addS3VolumeContextLatencyNs(Time.monotonicNowNanos() - start);
    return s3VolumeContext.build();
  }

  @Override
  public OmMultipartUploadListParts listParts(final String volumeName,
      final String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts)  throws IOException {

    ResolvedBucket bucket = resolveBucketLink(Pair.of(volumeName, bucketName));

    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.KEY, keyName);
    auditMap.put(OzoneConsts.UPLOAD_ID, uploadID);
    auditMap.put(OzoneConsts.PART_NUMBER_MARKER,
        Integer.toString(partNumberMarker));
    auditMap.put(OzoneConsts.MAX_PARTS, Integer.toString(maxParts));

    metrics.incNumListMultipartUploadParts();
    try {
      OmMultipartUploadListParts omMultipartUploadListParts =
          keyManager.listParts(bucket.realVolume(), bucket.realBucket(),
              keyName, uploadID, partNumberMarker, maxParts);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction
          .LIST_MULTIPART_UPLOAD_PARTS, auditMap));
      return omMultipartUploadListParts;
    } catch (IOException ex) {
      metrics.incNumListMultipartUploadPartFails();
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction
          .LIST_MULTIPART_UPLOAD_PARTS, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName,
      String prefix, String keyMarker, String uploadIdMarker, int maxUploads, boolean withPagination)
      throws IOException {

    ResolvedBucket bucket = resolveBucketLink(Pair.of(volumeName, bucketName));

    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.PREFIX, prefix);

    metrics.incNumListMultipartUploads();
    try {
      OmMultipartUploadList omMultipartUploadList = keyManager.listMultipartUploads(bucket.realVolume(),
          bucket.realBucket(), prefix, keyMarker, uploadIdMarker, maxUploads, withPagination);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_MULTIPART_UPLOADS, auditMap));
      return omMultipartUploadList;

    } catch (IOException ex) {
      metrics.incNumListMultipartUploadFails();
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_MULTIPART_UPLOADS, auditMap, ex));
      throw ex;
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader =
        getReader(args)) {
      return rcReader.get().getFileStatus(args);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader =
        getReader(args)) {
      return rcReader.get().lookupFile(args);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                          String startKey, long numEntries)
          throws IOException {
    return listStatus(args, recursive, startKey, numEntries, false);
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader =
        getReader(args)) {
      return rcReader.get().listStatus(
          args, recursive, startKey, numEntries, allowPartialPrefixes);
    }
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(OmKeyArgs args,
      boolean recursive, String startKey, long numEntries,
      boolean allowPartialPrefixes) throws IOException {
    List<OzoneFileStatus> ozoneFileStatuses =
        listStatus(args, recursive, startKey, numEntries, allowPartialPrefixes);

    return ozoneFileStatuses.stream()
        .map(OzoneFileStatusLight::fromOzoneFileStatus)
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader =
        getReader(obj)) {
      return rcReader.get().getAcl(obj);
    }
  }

  /**
   * Download and install latest checkpoint from leader OM.
   *
   * @param leaderId peerNodeID of the leader OM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  public synchronized TermIndex installSnapshotFromLeader(String leaderId) {
    if (omRatisSnapshotProvider == null) {
      LOG.error("OM Snapshot Provider is not configured as there are no peer " +
          "nodes.");
      return null;
    }

    DBCheckpoint omDBCheckpoint;
    try {
      omDBCheckpoint = omRatisSnapshotProvider.
          downloadDBSnapshotFromLeader(leaderId);
    } catch (IOException ex) {
      LOG.error("Failed to download snapshot from Leader {}.", leaderId,  ex);
      return null;
    }

    TermIndex termIndex = null;
    try {
      // Install hard links.
      OmSnapshotUtils.createHardLinks(omDBCheckpoint.getCheckpointLocation());
      termIndex = installCheckpoint(leaderId, omDBCheckpoint);
    } catch (Exception ex) {
      LOG.error("Failed to install snapshot from Leader OM.", ex);
    }
    return termIndex;
  }

  /**
   * Install checkpoint. If the checkpoint snapshot index is greater than
   * OM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied.
   */
  TermIndex installCheckpoint(String leaderId, DBCheckpoint omDBCheckpoint)
      throws Exception {

    Path checkpointLocation = omDBCheckpoint.getCheckpointLocation();
    TransactionInfo checkpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(configuration, checkpointLocation);

    LOG.info("Installing checkpoint with OMTransactionInfo {}",
        checkpointTrxnInfo);

    return installCheckpoint(leaderId, checkpointLocation, checkpointTrxnInfo);
  }

  TermIndex installCheckpoint(String leaderId, Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = Time.monotonicNow();
    File oldDBLocation = metadataManager.getStore().getDbLocation();
    try {
      // Stop Background services
      keyManager.stop();
      stopSecretManager();
      stopTrashEmptier();
      omSnapshotManager.invalidateCache();
      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      omRatisServer.getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Stop the checkpoint install process and restart the services.
      keyManager.start(configuration);
      startSecretManagerIfNecessary();
      startTrashEmptier(configuration);
      throw e;
    }

    File dbBackup = null;
    TermIndex termIndex = omRatisServer.getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();

    // Check if current applied log index is smaller than the downloaded
    // checkpoint transaction index. If yes, proceed by stopping the ratis
    // server so that the OM state can be re-initialized. If no then do not
    // proceed with installSnapshot.
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, checkpointLocation);

    boolean oldOmMetadataManagerStopped = false;
    boolean newMetadataManagerStarted = false;
    boolean omRpcServerStopped = false;
    long time = Time.monotonicNow();
    if (canProceed) {
      // Stop RPC server before stop metadataManager
      omRpcServer.stop();
      isOmRpcServerRunning = false;
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend " +
          (Time.monotonicNow() - time) + " ms.");
      try {
        // Stop old metadataManager before replacing DB Dir
        time = Time.monotonicNow();
        metadataManager.stop();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend " +
            (Time.monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        exitManager.exitSystem(1, errorMsg, e, LOG);
      }
      try {
        time = Time.monotonicNow();
        dbBackup = replaceOMDBWithCheckpoint(lastAppliedIndex,
            oldDBLocation, checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", leaderId, term, lastAppliedIndex,
            Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
            " DB with downloaded checkpoint. Reloading old OM state.",
            leaderId, e);
      }
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at TermIndex {} " +
          "and checkpoint has lower TermIndex {}. Reloading old state of OM.",
          termIndex, checkpointTrxnInfo.getTermIndex());
    }

    if (oldOmMetadataManagerStopped) {
      // Close snapDiff's rocksDB instance only if metadataManager gets closed.
      omSnapshotManager.close();
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      if (oldOmMetadataManagerStopped) {
        time = Time.monotonicNow();
        reloadOMState();
        setTransactionInfo(TransactionInfo.valueOf(termIndex));
        omRatisServer.getOmStateMachine().unpause(lastAppliedIndex, term);
        newMetadataManagerStarted = true;
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, Time.monotonicNow() - time);
      } else {
        // OM DB is not stopped. Start the services.
        keyManager.start(configuration);
        startSecretManagerIfNecessary();
        startTrashEmptier(configuration);
        omRatisServer.getOmStateMachine().unpause(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      exitManager.exitSystem(1, errorMsg, ex, LOG);
    }

    if (omRpcServerStopped && newMetadataManagerStarted) {
      // Start the RPC server. RPC server start requires metadataManager
      try {
        time = Time.monotonicNow();
        omRpcServer = getRpcServer(configuration);
        omRpcServer.start();
        isOmRpcServerRunning = true;
        LOG.info("RPC server is re-started. Spend " +
            (Time.monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        exitManager.exitSystem(1, errorMsg, e, LOG);
      }
    }

    // Delete the backup DB
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup, e);
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Install Snapshot failed and old state was reloaded. Return null to
      // Ratis to indicate that installation failed.
      return null;
    }

    // TODO: We should only return the snpashotIndex to the leader.
    //  Should be fixed after RATIS-586
    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (Time.monotonicNow() - startTime));
    return newTermIndex;
  }

  private void stopTrashEmptier() {
    if (this.emptier != null) {
      emptier.interrupt();
      emptier = null;
    }
  }

  /**
   * Replace the current OM DB with the new DB checkpoint.
   *
   * @param lastAppliedIndex the last applied index in the current OM DB.
   * @param checkpointPath   path to the new DB checkpoint
   * @return location of backup of the original DB
   */
  File replaceOMDBWithCheckpoint(long lastAppliedIndex, File oldDB,
      Path checkpointPath) throws IOException {

    // Take a backup of the current DB
    String dbBackupName = OzoneConsts.OM_DB_BACKUP_PREFIX +
        lastAppliedIndex + "_" + System.currentTimeMillis();
    File dbDir = oldDB.getParentFile();

    // Backup the active fs and snapshot dirs.
    File dbBackupDir = new File(dbDir, dbBackupName);
    if (!dbBackupDir.mkdirs()) {
      throw new IOException("Failed to make db backup dir: " +
          dbBackupDir);
    }
    File dbBackup = new File(dbBackupDir, oldDB.getName());
    File dbSnapshotsDir = new File(dbDir, OM_SNAPSHOT_DIR);
    File dbSnapshotsBackup = new File(dbBackupDir, OM_SNAPSHOT_DIR);
    Files.move(oldDB.toPath(), dbBackup.toPath());
    if (dbSnapshotsDir.exists()) {
      Files.move(dbSnapshotsDir.toPath(),
          dbSnapshotsBackup.toPath());
    }

    moveCheckpointFiles(oldDB, checkpointPath, dbDir, dbBackup, dbSnapshotsDir,
        dbSnapshotsBackup);
    return dbBackupDir;
  }

  private void moveCheckpointFiles(File oldDB, Path checkpointPath, File dbDir,
                                   File dbBackup, File dbSnapshotsDir,
                                   File dbSnapshotsBackup) throws IOException {
    // Move the new DB checkpoint into the om metadata dir
    Path markerFile = new File(dbDir, DB_TRANSIENT_MARKER).toPath();
    try {
      // Create a Transient Marker file. This file will be deleted if the
      // checkpoint DB is successfully moved to the old DB location or if the
      // old DB backup is reset to its location. If not, then the OM DB is in
      // an inconsistent state and this marker file will fail OM from
      // starting up.
      Files.createFile(markerFile);
      // Link each of the candidate DB files to real DB directory.  This
      // preserves the links that already exist between files in the
      // candidate db.
      OmSnapshotUtils.linkFiles(checkpointPath.toFile(),
          oldDB);
      moveOmSnapshotData(oldDB.toPath(), dbSnapshotsDir.toPath());
      Files.deleteIfExists(markerFile);
    } catch (IOException e) {
      LOG.error("Failed to move downloaded DB checkpoint {} to metadata " +
              "directory {}. Exception: {}. Resetting to original DB.",
          checkpointPath, oldDB.toPath(), e);
      try {
        FileUtil.fullyDelete(oldDB);
        Files.move(dbBackup.toPath(), oldDB.toPath());
        if (dbSnapshotsBackup.exists()) {
          Files.move(dbSnapshotsBackup.toPath(), dbSnapshotsDir.toPath());
        }
        Files.deleteIfExists(markerFile);
      } catch (IOException ex) {
        String errorMsg = "Failed to reset to original DB. OM is in an " +
            "inconsistent state.";
        exitManager.exitSystem(1, errorMsg, ex, LOG);
      }
      throw e;
    }
  }

  // Move the new snapshot directory into place.
  private void moveOmSnapshotData(Path dbPath, Path dbSnapshotsDir)
      throws IOException {
    Path incomingSnapshotsDir = Paths.get(dbPath.toString(),
        OM_SNAPSHOT_DIR);
    if (incomingSnapshotsDir.toFile().exists()) {
      Files.move(incomingSnapshotsDir, dbSnapshotsDir);
    }
  }

  /**
   * Re-instantiate MetadataManager with new DB checkpoint.
   * All the classes which use/ store MetadataManager should also be updated
   * with the new MetadataManager instance.
   */
  private void reloadOMState() throws IOException {
    instantiateServices(true);

    // Restart required services
    metadataManager.start(configuration);
    keyManager.start(configuration);
    startSecretManagerIfNecessary();
    startTrashEmptier(configuration);

    // Set metrics and start metrics background thread
    metrics.setNumVolumes(metadataManager.countRowsInTable(metadataManager
        .getVolumeTable()));
    metrics.setNumBuckets(metadataManager.countRowsInTable(metadataManager
        .getBucketTable()));
    metrics.setNumKeys(metadataManager.countEstimatedRowsInTable(metadataManager
        .getKeyTable(getBucketLayout())));

    // FSO(FILE_SYSTEM_OPTIMIZED)
    metrics.setNumDirs(metadataManager
        .countEstimatedRowsInTable(metadataManager.getDirectoryTable()));
    metrics.setNumFiles(metadataManager
        .countEstimatedRowsInTable(metadataManager.getFileTable()));

    // Delete the omMetrics file if it exists and save a new metrics file
    // with new data
    Files.deleteIfExists(getMetricsStorageFile().toPath());
    saveOmMetrics();
  }

  public static Logger getLogger() {
    return LOG;
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  public OmConfig getConfig() {
    return config;
  }

  @VisibleForTesting
  public void setConfiguration(OzoneConfiguration conf) {
    this.configuration = conf;
    config = conf.getObject(OmConfig.class);
  }

  public OzoneConfiguration reloadConfiguration() {
    if (testReloadConfigFlag) {
      // If this flag is set, do not reload config
      return this.configuration;
    }
    return new OzoneConfiguration();
  }

  public static void setTestReloadConfigFlag(boolean testReloadConfigFlag) {
    OzoneManager.testReloadConfigFlag = testReloadConfigFlag;
  }

  public static void setTestSecureOmFlag(boolean testSecureOmFlag) {
    OzoneManager.testSecureOmFlag = testSecureOmFlag;
  }

  @VisibleForTesting
  public static void setUgi(UserGroupInformation user) {
    OzoneManager.testUgi = user;
  }

  public OMNodeDetails getNodeDetails() {
    return omNodeDetails;
  }

  public String getOMNodeId() {
    return omNodeDetails.getNodeId();
  }

  public String getOMServiceId() {
    return omNodeDetails.getServiceId();
  }

  @VisibleForTesting
  public List<OMNodeDetails> getPeerNodes() {
    return new ArrayList<>(peerNodesMap.values());
  }

  public OMNodeDetails getPeerNode(String nodeID) {
    return peerNodesMap.get(nodeID);
  }

  @VisibleForTesting
  public CertificateClient getCertificateClient() {
    return certClient;
  }

  public String getComponent() {
    return omComponent;
  }

  /**
   * Return maximum volumes count per user.
   *
   * @return maxUserVolumeCount
   */
  public long getMaxUserVolumeCount() {
    return config.getMaxUserVolumeCount();
  }
  /**
   * Return true, if the current OM node is leader and in ready state to
   * process the requests.
   *
   * If ratis is not enabled, then it always returns true.
   */
  public boolean isLeaderReady() {
    final OzoneManagerRatisServer ratisServer = omRatisServer;
    return ratisServer != null && ratisServer.getLeaderStatus() == LEADER_AND_READY;
  }

  /**
   * Checks the leader status.  Does nothing if this OM is leader and is ready.
   * @throws OMLeaderNotReadyException  if leader, but not ready
   * @throws OMNotLeaderException       if not leader
   */
  public void checkLeaderStatus() throws OMNotLeaderException,
      OMLeaderNotReadyException {
    OzoneManagerRatisServer.RaftServerStatus raftServerStatus =
        omRatisServer.getLeaderStatus();
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    switch (raftServerStatus) {
    case LEADER_AND_READY: return;
    case LEADER_AND_NOT_READY:
      throw new OMLeaderNotReadyException(raftPeerId + " is Leader but not ready to process request yet.");
    case NOT_LEADER:
      throw omRatisServer.newOMNotLeaderException();
    default: throw new IllegalStateException(
        "Unknown Ratis Server state: " + raftServerStatus);
    }
  }

  /**
   * @return true if Ozone filesystem snapshot is enabled, false otherwise.
   */
  public boolean isFilesystemSnapshotEnabled() {
    return fsSnapshotEnabled;
  }

  /**
   * Get DB updates since a specific sequence number.
   *
   * @param dbUpdatesRequest request that encapsulates a sequence number.
   * @return Wrapper containing the updates.
   * @throws SequenceNumberNotFoundException if db is unable to read the data.
   */
  @Override
  public DBUpdates getDBUpdates(
      DBUpdatesRequest dbUpdatesRequest)
      throws IOException {
    long limitCount = Long.MAX_VALUE;
    if (dbUpdatesRequest.hasLimitCount()) {
      limitCount = dbUpdatesRequest.getLimitCount();
    }
    DBUpdatesWrapper updatesSince = metadataManager.getStore()
        .getUpdatesSince(dbUpdatesRequest.getSequenceNumber(), limitCount);
    DBUpdates dbUpdates = new DBUpdates(updatesSince.getData());
    dbUpdates.setCurrentSequenceNumber(updatesSince.getCurrentSequenceNumber());
    dbUpdates.setLatestSequenceNumber(updatesSince.getLatestSequenceNumber());
    dbUpdates.setDBUpdateSuccess(updatesSince.isDBUpdateSuccess());
    return dbUpdates;
  }

  public OzoneDelegationTokenSecretManager getDelegationTokenMgr() {
    return delegationTokenMgr;
  }

  /**
   * Return the list of Ozone administrators in effect.
   */
  public Collection<String> getOmAdminUsernames() {
    return omAdmins.getAdminUsernames();
  }

  public Collection<String> getOmReadOnlyAdminUsernames() {
    return readOnlyAdmins.getAdminUsernames();
  }

  public Collection<String> getOmAdminGroups() {
    return omAdmins.getAdminGroups();
  }

  /**
   * @param callerUgi Caller UserGroupInformation
   * @return return true if the {@code ugi} is a read-only OM admin
   */
  public boolean isReadOnlyAdmin(UserGroupInformation callerUgi) {
    return callerUgi != null && readOnlyAdmins.isAdmin(callerUgi);
  }

  /**
   * Return true if a UserGroupInformation is OM admin, false otherwise.
   * @param callerUgi Caller UserGroupInformation
   */
  public boolean isAdmin(UserGroupInformation callerUgi) {
    return callerUgi != null && omAdmins.isAdmin(callerUgi);
  }

  /**
   * Check ozone admin privilege, throws exception if not admin.
   */
  private void checkAdminUserPrivilege(String operation) throws IOException {
    final UserGroupInformation ugi = getRemoteUser();
    if (!isAdmin(ugi)) {
      throw new OMException("Only Ozone admins are allowed to " + operation,
          PERMISSION_DENIED);
    }
  }

  public boolean isS3Admin(UserGroupInformation callerUgi) {
    return OzoneAdmins.isS3Admin(callerUgi, s3OzoneAdmins);
  }

  @VisibleForTesting
  public boolean isRunning() {
    return omState == State.RUNNING;
  }

  public ResolvedBucket resolveBucketLink(KeyArgs args,
      OMClientRequest omClientRequest) throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()), omClientRequest);
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested)
      throws IOException {
    return resolveBucketLink(requested, false);
  }

  public ResolvedBucket resolveBucketLink(OmKeyArgs args)
      throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()));
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested,
      OMClientRequest omClientRequest)
      throws IOException {
    OmBucketInfo resolved;
    if (isAclEnabled) {
      resolved = resolveBucketLink(requested, new HashSet<>(),
              omClientRequest.createUGIForApi(),
              omClientRequest.getRemoteAddress(),
              omClientRequest.getHostName(),
              false);
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null, false);
    }
    return new ResolvedBucket(requested.getLeft(), requested.getRight(),
        resolved);
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested,
                                          boolean allowDanglingBuckets) throws IOException {
    return resolveBucketLink(requested, allowDanglingBuckets, isAclEnabled);
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested,
                                          boolean allowDanglingBuckets,
                                          boolean aclEnabled)
      throws IOException {
    OmBucketInfo resolved;
    if (aclEnabled) {
      UserGroupInformation ugi = getRemoteUser();
      if (getS3Auth() != null) {
        ugi = UserGroupInformation.createRemoteUser(
            OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId()));
      }
      InetAddress remoteIp = Server.getRemoteIp();
      resolved = resolveBucketLink(requested, new HashSet<>(),
          ugi,
          remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
          remoteIp != null ? remoteIp.getHostName() :
              omRpcAddress.getHostName(), allowDanglingBuckets, aclEnabled);
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null, allowDanglingBuckets, aclEnabled);
    }
    return new ResolvedBucket(requested.getLeft(), requested.getRight(),
        resolved);
  }

  private OmBucketInfo resolveBucketLink(
      Pair<String, String> volumeAndBucket,
      Set<Pair<String, String>> visited,
      UserGroupInformation userGroupInformation,
      InetAddress remoteAddress,
      String hostName,
      boolean allowDanglingBuckets) throws IOException {
    return resolveBucketLink(volumeAndBucket, visited, userGroupInformation, remoteAddress, hostName,
        allowDanglingBuckets, isAclEnabled);
  }

  /**
   * Resolves bucket symlinks. Read permission is required for following links.
   *
   * @param volumeAndBucket the bucket to be resolved (if it is a link)
   * @param visited collects link buckets visited during the resolution to
   *   avoid infinite loops
   * @return bucket location possibly updated with its actual volume and bucket
   *   after following bucket links
   * @throws IOException (most likely OMException) if ACL check fails, bucket is
   *   not found, loop is detected in the links, etc.
   */
  private OmBucketInfo resolveBucketLink(
      Pair<String, String> volumeAndBucket,
      Set<Pair<String, String>> visited,
      UserGroupInformation userGroupInformation,
      InetAddress remoteAddress,
      String hostName,
      boolean allowDanglingBuckets,
      boolean aclEnabled) throws IOException {

    String volumeName = volumeAndBucket.getLeft();
    String bucketName = volumeAndBucket.getRight();
    OmBucketInfo info;
    try {
      info = bucketManager.getBucketInfo(volumeName, bucketName);
    } catch (OMException e) {
      LOG.warn("Bucket {} not found in volume {}", bucketName, volumeName);
      if (allowDanglingBuckets) {
        return null;
      }
      throw e;
    }
    if (!info.isLink()) {
      return info;
    }

    if (!visited.add(volumeAndBucket)) {
      throw new OMException("Detected loop in bucket links",
          DETECTED_LOOP_IN_BUCKET_LINKS);
    }

    if (aclEnabled) {
      final ACLType type = ACLType.READ;
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, type,
          volumeName, bucketName, null, userGroupInformation,
          remoteAddress, hostName, true,
          getVolumeOwner(volumeName, type, ResourceType.BUCKET));
    }

    return resolveBucketLink(
        Pair.of(info.getSourceVolume(), info.getSourceBucket()),
        visited, userGroupInformation, remoteAddress, hostName,
        allowDanglingBuckets, aclEnabled);
  }

  @VisibleForTesting
  public void setExitManagerForTesting(ExitManager exitManagerForTesting) {
    exitManager = exitManagerForTesting;
  }

  public boolean getEnableFileSystemPaths() {
    return config.isFileSystemPathEnabled();
  }

  public boolean getKeyPathLockEnabled() {
    return configuration.getBoolean(OZONE_OM_KEY_PATH_LOCK_ENABLED,
        OZONE_OM_KEY_PATH_LOCK_ENABLED_DEFAULT);
  }

  public OzoneLockProvider getOzoneLockProvider() {
    return this.ozoneLockProvider;
  }

  public ReplicationConfig getDefaultReplicationConfig() {
    if (defaultReplicationConfig == null) {
      setReplicationFromConfig();
    }
    return defaultReplicationConfig;
  }

  public void setReplicationFromConfig() {
    final String replication = configuration.getTrimmed(
        OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT);
    final String type = configuration.getTrimmed(
        OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        OZONE_SERVER_DEFAULT_REPLICATION_TYPE_DEFAULT);
    defaultReplicationConfig = ReplicationConfig.parse(ReplicationType.valueOf(type),
        replication, configuration);
    LOG.info("Set default replication in OM: {}/{} -> {}", type, replication, defaultReplicationConfig);
  }

  public BucketLayout getOMDefaultBucketLayout() {
    return this.defaultBucketLayout;
  }

  /**
   * Create volume which is required for S3Gateway operations.
   */
  private void addS3GVolumeToDB() throws IOException {
    String s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(configuration);
    String dbVolumeKey = metadataManager.getVolumeKey(s3VolumeName);

    if (!s3VolumeName.equals(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT)) {
      LOG.warn("Make sure that all S3Gateway use same volume name." +
          " Otherwise user need to manually create/configure Volume " +
          "configured by S3Gateway");
    }
    if (!metadataManager.getVolumeTable().isExist(dbVolumeKey)) {
      // the highest transaction ID is reserved for this operation.
      long transactionID = MAX_TRXN_ID + 1;
      long objectID = OmUtils.addEpochToTxId(metadataManager.getOmEpoch(),
          transactionID);
      String userName =
          UserGroupInformation.getCurrentUser().getShortUserName();

      // Add volume and user info to DB and cache.

      OmVolumeArgs omVolumeArgs = createS3VolumeContext(s3VolumeName, objectID);

      String dbUserKey = metadataManager.getUserKey(userName);
      PersistedUserVolumeInfo userVolumeInfo =
          PersistedUserVolumeInfo.newBuilder()
          .setObjectID(objectID)
          .setUpdateID(transactionID)
          .addVolumeNames(s3VolumeName).build();


      // Commit to DB.
      try (BatchOperation batchOperation =
          metadataManager.getStore().initBatchOperation()) {
        metadataManager.getVolumeTable().putWithBatch(batchOperation,
            dbVolumeKey, omVolumeArgs);

        metadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
            userVolumeInfo);

        metadataManager.getStore().commitBatchOperation(batchOperation);
      }

      // Add to cache.
      metadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          CacheValue.get(transactionID, omVolumeArgs));
      metadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(dbUserKey),
          CacheValue.get(transactionID, userVolumeInfo));
      LOG.info("Created Volume {} With Owner {} required for S3Gateway " +
              "operations.", s3VolumeName, userName);
    }
  }

  private OmVolumeArgs createS3VolumeContext(String s3Volume,
      long objectID) throws IOException {
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    long time = Time.now();

    // We need to set the updateID to DEFAULT_OM_UPDATE_ID, because when
    // acl op on S3v volume during updateID check it will fail if we have a
    // value with maximum transactionID. Because updateID checks if new
    // new updateID is greater than previous updateID, otherwise it fails.

    OmVolumeArgs.Builder omVolumeArgs = new OmVolumeArgs.Builder()
        .setVolume(s3Volume)
        .setUpdateID(DEFAULT_OM_UPDATE_ID)
        .setObjectID(objectID)
        .setCreationTime(time)
        .setModificationTime(time)
        .setOwnerName(userName)
        .setAdminName(userName)
        .setQuotaInBytes(OzoneConsts.QUOTA_RESET);

    // Provide ACLType of ALL which is default acl rights for user and group.
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    //User ACL
    listOfAcls.add(OzoneAcl.of(ACLIdentityType.USER,
        userName, ACCESS, ACLType.ALL));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(userName).getGroupNames());

    userGroups.forEach((group) -> listOfAcls.add(
        OzoneAcl.of(ACLIdentityType.GROUP, group, ACCESS, ACLType.ALL)));

    // Add ACLs
    for (OzoneAcl ozoneAcl : listOfAcls) {
      omVolumeArgs.addOzoneAcls(ozoneAcl);
    }

    return omVolumeArgs.build();
  }

  public OMLayoutVersionManager getVersionManager() {
    return versionManager;
  }

  public OzoneManagerPrepareState getPrepareState() {
    return prepareState;
  }

  /**
   * Determines if the prepare gate should be enabled on this OM after OM
   * is restarted.
   * This must be done after metadataManager is instantiated
   * and before the RPC server is started.
   */
  private void instantiatePrepareStateOnStartup()
      throws IOException {
    TransactionInfo txnInfo = metadataManager.getTransactionInfoTable()
        .get(TRANSACTION_INFO_KEY);
    if (txnInfo == null) {
      // No prepare request could be received if there are not transactions.
      prepareState = new OzoneManagerPrepareState(configuration);
    } else {
      prepareState = new OzoneManagerPrepareState(configuration,
          txnInfo.getTransactionIndex());
      TransactionInfo dbPrepareValue =
          metadataManager.getTransactionInfoTable().get(PREPARE_MARKER_KEY);

      boolean hasMarkerFile =
          (prepareState.getState().getStatus() ==
              PrepareStatus.PREPARE_COMPLETED);
      boolean hasDBMarker = (dbPrepareValue != null);

      if (hasDBMarker) {
        long dbPrepareIndex = dbPrepareValue.getTransactionIndex();

        if (hasMarkerFile) {
          long prepareFileIndex = prepareState.getState().getIndex();
          // If marker and DB prepare index do not match, use the DB value
          // since this is synced through Ratis, to avoid divergence.
          if (prepareFileIndex != dbPrepareIndex) {
            LOG.warn("Prepare marker file index {} does not match DB prepare " +
                "index {}. Writing DB index to prepare file and maintaining " +
                "prepared state.", prepareFileIndex, dbPrepareIndex);
            prepareState.finishPrepare(dbPrepareIndex);
          }
          // Else, marker and DB are present and match, so OM is prepared.
        } else {
          // Prepare cancelled with startup flag to remove marker file.
          // Persist this to the DB.
          // If the startup flag is used it should be used on all OMs to avoid
          // divergence.
          metadataManager.getTransactionInfoTable().delete(PREPARE_MARKER_KEY);
        }
      } else if (hasMarkerFile) {
        // Marker file present but no DB entry present.
        // This should never happen. If a prepare request fails partway
        // through, OM should replay it so both the DB and marker file exist.
        throw new OMException("Prepare marker file found on startup without " +
            "a corresponding database entry. Corrupt prepare state.",
            ResultCodes.PREPARE_FAILED);
      }
      // Else, no DB or marker file, OM is not prepared.
    }
  }

  /**
   * Determines if the prepare gate should be enabled on this OM after OM
   * receives a snapshot.
   */
  private void instantiatePrepareStateAfterSnapshot()
      throws IOException {
    TransactionInfo txnInfo = metadataManager.getTransactionInfoTable()
        .get(TRANSACTION_INFO_KEY);
    if (txnInfo == null) {
      // No prepare request could be received if there are not transactions.
      prepareState = new OzoneManagerPrepareState(configuration);
    } else {
      prepareState = new OzoneManagerPrepareState(configuration,
          txnInfo.getTransactionIndex());
      TransactionInfo dbPrepareValue =
          metadataManager.getTransactionInfoTable().get(PREPARE_MARKER_KEY);

      boolean hasDBMarker = (dbPrepareValue != null);

      if (hasDBMarker) {
        // Snapshot contained a prepare request to apply.
        // Update the in memory prepare gate and marker file index.
        // If we have already done this, the operation is idempotent.
        long dbPrepareIndex = dbPrepareValue.getTransactionIndex();
        prepareState.restorePrepareFromIndex(dbPrepareIndex,
            txnInfo.getTransactionIndex());
      } else {
        // No DB marker.
        // Deletes marker file if exists, otherwise does nothing if we were not
        // already prepared.
        prepareState.cancelPrepare();
      }
    }
  }

  public int getMinMultipartUploadPartSize() {
    return minMultipartUploadPartSize;
  }

  @VisibleForTesting
  public void setMinMultipartUploadPartSize(int partSizeForTest) {
    this.minMultipartUploadPartSize = partSizeForTest;
  }

  @VisibleForTesting
  public boolean isOmRpcServerRunning() {
    return isOmRpcServerRunning;
  }

  @Override
  public EchoRPCResponse echoRPCReq(byte[] payloadReq, int payloadSizeResp,
                                    boolean writeToRatis) {
    return null;
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName, String keyName, boolean force) {
    return null;
  }

  @Override
  public void setTimes(OmKeyArgs keyArgs, long mtime, long atime)
      throws IOException {
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    switch (action) {
    case ENTER:
      throw new OMException("Enter safe mode is unsupported",
          INTERNAL_ERROR);
    case FORCE_EXIT:
      return getScmClient().getContainerClient().forceExitSafeMode();
    case GET:
      return getScmClient().getContainerClient().inSafeMode();
    //case LEAVE:
    // TODO: support LEAVE in the future.
    default:
      throw new OMException("Unsupported safe mode action " + action,
          INTERNAL_ERROR);
    }
  }

  @Override
  public String getQuotaRepairStatus() throws IOException {
    checkAdminUserPrivilege("quota repair status");
    return QuotaRepairTask.getStatus();
  }

  @Override
  public void startQuotaRepair(List<String> buckets) throws IOException {
    checkAdminUserPrivilege("start quota repair");
    new QuotaRepairTask(this).repair(buckets);
  }

  @Override
  public Map<String, String> getObjectTagging(final OmKeyArgs args)
      throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcReader = getReader(args)) {
      return rcReader.get().getObjectTagging(args);
    }
  }


  /**
   * Write down Layout version of a finalized feature to DB on finalization.
   * @param lvm OMLayoutVersionManager
   * @param omMetadataManager omMetadataManager instance
   * @throws IOException on Error.
   */
  private void updateLayoutVersionInDB(OMLayoutVersionManager lvm,
                                       OMMetadataManager omMetadataManager)
      throws IOException {
    omMetadataManager.getMetaTable().put(LAYOUT_VERSION_KEY,
        String.valueOf(lvm.getMetadataLayoutVersion()));
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  void saveNewCertId(String certId) {
    try {
      omStorage.setOmCertSerialId(certId);
      omStorage.persistCurrentState();
    } catch (IOException ex) {
      // New cert ID cannot be persisted into VERSION file.
      LOG.error("Failed to persist new cert ID {} to VERSION file." +
          "Terminating OzoneManager...", certId, ex);
      shutDown("OzoneManage shutdown because VERSION file persist failure.");
    }
  }

  public static HddsProtos.OzoneManagerDetailsProto getOmDetailsProto(
      ConfigurationSource config, String omID) {
    boolean flexibleFqdnResolutionEnabled = config.getBoolean(
        OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED,
        OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT);
    InetSocketAddress omRpcAdd = OmUtils.getOmAddress(config);
    String ip = null;

    boolean addressResolved = omRpcAdd != null && omRpcAdd.getAddress() != null;
    if (flexibleFqdnResolutionEnabled && !addressResolved && omRpcAdd != null) {
      InetSocketAddress omRpcAddWithHostName =
          OzoneNetUtils.getAddressWithHostNameLocal(omRpcAdd);
      if (omRpcAddWithHostName != null
          && omRpcAddWithHostName.getAddress() != null) {
        addressResolved = true;
        ip = omRpcAddWithHostName.getAddress().getHostAddress();
      }
    }

    if (!addressResolved) {
      LOG.error("Incorrect om rpc address. omRpcAdd:{}", omRpcAdd);
      throw new RuntimeException("Can't get SCM signed certificate. " +
          "omRpcAdd: " + omRpcAdd);
    }

    if (ip == null) {
      ip = omRpcAdd.getAddress().getHostAddress();
    }

    String hostname = omRpcAdd.getHostName();
    int port = omRpcAdd.getPort();

    HddsProtos.OzoneManagerDetailsProto.Builder omDetailsProtoBuilder =
        HddsProtos.OzoneManagerDetailsProto.newBuilder()
            .setHostName(hostname)
            .setIpAddress(ip)
            .setUuid(omID)
            .addPorts(HddsProtos.Port.newBuilder()
                .setName(RPC_PORT)
                .setValue(port)
                .build());

    HddsProtos.OzoneManagerDetailsProto omDetailsProto =
        omDetailsProtoBuilder.build();
    LOG.info("OzoneManager ports added:{}", omDetailsProto.getPortsList());
    return omDetailsProto;
  }

  /**
   * Get a referenced counted OmMetadataReader instance.
   * Caller is responsible of closing the return value.
   * Using try-with-resources is recommended.
   * @param keyArgs OmKeyArgs
   * @return ReferenceCounted<IOmMetadataReader, SnapshotCache>
   */
  private ReferenceCounted<IOmMetadataReader> getReader(OmKeyArgs keyArgs)
      throws IOException {
    return omSnapshotManager.getActiveFsMetadataOrSnapshot(
        keyArgs.getVolumeName(), keyArgs.getBucketName(), keyArgs.getKeyName());
  }

  /**
   * Get a referenced counted OmMetadataReader instance.
   * Caller is responsible of closing the return value.
   * Using try-with-resources is recommended.
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param key key path
   * @return ReferenceCounted<IOmMetadataReader, SnapshotCache>
   */
  private ReferenceCounted<IOmMetadataReader> getReader(
          String volumeName, String bucketName, String key) throws IOException {
    return omSnapshotManager.getActiveFsMetadataOrSnapshot(
        volumeName, bucketName, key);
  }

  /**
   * Get a referenced counted OmMetadataReader instance.
   * Caller is responsible of closing the return value.
   * Using try-with-resources is recommended.
   * @param ozoneObj OzoneObj
   * @return ReferenceCounted<IOmMetadataReader, SnapshotCache>
   */
  private ReferenceCounted<IOmMetadataReader> getReader(OzoneObj ozoneObj)
      throws IOException {
    return omSnapshotManager.getActiveFsMetadataOrSnapshot(
        ozoneObj.getVolumeName(),
        ozoneObj.getBucketName(),
        ozoneObj.getKeyName());
  }

  @Override
  @SuppressWarnings("parameternumber")
  public SnapshotDiffResponse snapshotDiff(String volume,
                                           String bucket,
                                           String fromSnapshot,
                                           String toSnapshot,
                                           String token,
                                           int pageSize,
                                           boolean forceFullDiff,
                                           boolean disableNativeDiff)
      throws IOException {
    // Updating the volumeName & bucketName in case the bucket is a linked bucket. We need to do this before a
    // permission check, since linked bucket permissions and source bucket permissions could be different.
    ResolvedBucket resolvedBucket = resolveBucketLink(Pair.of(volume, bucket), false);
    return omSnapshotManager.getSnapshotDiffReport(resolvedBucket.realVolume(), resolvedBucket.realBucket(),
        fromSnapshot, toSnapshot, token, pageSize, forceFullDiff, disableNativeDiff);
  }

  @Override
  public CancelSnapshotDiffResponse cancelSnapshotDiff(String volume,
                                                       String bucket,
                                                       String fromSnapshot,
                                                       String toSnapshot)
      throws IOException {
    ResolvedBucket resolvedBucket = this.resolveBucketLink(Pair.of(volume, bucket), false);
    return omSnapshotManager.cancelSnapshotDiff(resolvedBucket.realVolume(), resolvedBucket.realBucket(),
        fromSnapshot, toSnapshot);
  }

  @Override
  public List<SnapshotDiffJob> listSnapshotDiffJobs(String volume,
                                                    String bucket,
                                                    String jobStatus,
                                                    boolean listAll)
      throws IOException {
    ResolvedBucket resolvedBucket = this.resolveBucketLink(Pair.of(volume, bucket), false);
    if (isAclEnabled) {
      omMetadataReader.checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.LIST, volume, bucket, null);
    }

    return omSnapshotManager.getSnapshotDiffList(resolvedBucket.realVolume(), resolvedBucket.realBucket(),
        jobStatus, listAll);
  }

  @Override
  public String printCompactionLogDag(String fileNamePrefix,
                                      String graphType)
      throws IOException {
    checkAdminUserPrivilege("print compaction DAG.");

    if (StringUtils.isBlank(fileNamePrefix)) {
      fileNamePrefix = "dag-";
    } else {
      fileNamePrefix = fileNamePrefix + "-";
    }
    File tempFile = File.createTempFile(fileNamePrefix, ".png");

    PrintableGraph.GraphType type;

    try {
      type = PrintableGraph.GraphType.valueOf(graphType);
    } catch (IllegalArgumentException e) {
      type = FILE_NAME;
    }

    getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .pngPrintMutableGraph(tempFile.getAbsolutePath(), type);

    return String.format("Graph was generated at '\\tmp\\%s' on OM " +
        "node '%s'.", tempFile.getName(), getOMNodeId());
  }

  private String reconfOzoneAdmins(String newVal) {
    getConfiguration().set(OZONE_ADMINISTRATORS, newVal);
    Collection<String> admins =
        OzoneAdmins.getOzoneAdminsFromConfig(getConfiguration(),
            omStarterUser);
    omAdmins.setAdminUsernames(admins);
    LOG.info("Load conf {} : {}, and now admins are: {}", OZONE_ADMINISTRATORS,
        newVal, admins);
    return String.valueOf(newVal);
  }

  private String reconfOzoneReadOnlyAdmins(String newVal) {
    getConfiguration().set(OZONE_READONLY_ADMINISTRATORS, newVal);
    Collection<String> pReadOnlyAdmins =
        OzoneAdmins.getOzoneReadOnlyAdminsFromConfig(getConfiguration());
    readOnlyAdmins.setAdminUsernames(pReadOnlyAdmins);
    LOG.info("Load conf {} : {}, and now readOnly admins are: {}",
        OZONE_READONLY_ADMINISTRATORS, newVal, pReadOnlyAdmins);
    return String.valueOf(newVal);
  }

  private String reconfOzoneKeyDeletingLimitPerTask(String newVal) {
    Preconditions.checkArgument(Integer.parseInt(newVal) >= 0,
        OZONE_KEY_DELETING_LIMIT_PER_TASK + " cannot be negative.");
    getConfiguration().set(OZONE_KEY_DELETING_LIMIT_PER_TASK, newVal);

    getKeyManager().getDeletingService()
        .setKeyLimitPerTask(Integer.parseInt(newVal));
    return newVal;
  }

  private String reconfigureAllowListAllVolumes(String newVal) {
    getConfiguration().set(OZONE_OM_VOLUME_LISTALL_ALLOWED, newVal);
    setAllowListAllVolumesFromConfig();
    return String.valueOf(allowListAllVolumes);
  }

  public void validateReplicationConfig(ReplicationConfig replicationConfig)
      throws OMException {
    try {
      getReplicationConfigValidator().validate(replicationConfig);
    } catch (IllegalArgumentException e) {
      throw new OMException("Invalid replication config: " + replicationConfig,
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  @VisibleForTesting
  public ReplicationConfigValidator getReplicationConfigValidator() {
    return replicationConfigValidator;
  }

  @VisibleForTesting
  public ReconfigurationHandler getReconfigurationHandler() {
    return reconfigurationHandler;
  }

  /**
   * Wait until both buffers are flushed.  This is used in cases like
   * "follower bootstrap tarball creation" where the rocksDb for the active
   * fs needs to synchronized with the rocksdb's for the snapshots.
   */
  public void awaitDoubleBufferFlush() throws InterruptedException {
    getOmRatisServer().getOmStateMachine().awaitDoubleBufferFlush();
  }

  public void checkFeatureEnabled(OzoneManagerVersion feature) throws OMException {
    String disabledFeatures = configuration.get(OMConfigKeys.OZONE_OM_FEATURES_DISABLED, "");
    if (disabledFeatures.contains(feature.name())) {
      throw new OMException("Feature disabled: " + feature, OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }

  public void compactOMDB(String columnFamily) throws IOException {
    checkAdminUserPrivilege("compact column family " + columnFamily);
    new CompactDBService(this).compact(columnFamily);
  }

  public OMExecutionFlow getOmExecutionFlow() {
    return omExecutionFlow;
  }
}
