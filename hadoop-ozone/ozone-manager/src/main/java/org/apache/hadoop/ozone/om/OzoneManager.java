/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import javax.management.ObjectName;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.multitenant.OMRangerBGSyncService;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBUpdatesWrapper;
import org.apache.hadoop.hdds.utils.db.SequenceNumberNotFoundException;
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
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.protocol.OMInterServiceProtocol;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolPB;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.common.ha.ratis.RatisSnapshotInfo;
import org.apache.hadoop.hdds.security.OzoneSecurityException;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.snapshot.OzoneManagerSnapshotProvider;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMUpgradeFinalizer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OzoneManagerAdminService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedUserAccessIdInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.protocolPB.OMInterServiceProtocolServerSideImpl;
import org.apache.hadoop.ozone.protocolPB.OMAdminProtocolServerSideImpl;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmInfo;
import static org.apache.hadoop.ozone.OmUtils.MAX_TRXN_ID;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FLEXIBLE_FQDN_RESOLUTION_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_TEMP_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.PREPARE_MARKER_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.RPC_PORT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GRPC_SERVER_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.getRaftGroupIdFromOmServiceId;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.OzoneManagerInterService;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneManagerService;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone Manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class OzoneManager extends ServiceRuntimeInfoImpl
    implements OzoneManagerProtocol, OMInterServiceProtocol,
    OMMXBean, Auditor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final String OM_DAEMON = "om";

  // This is set for read requests when OMRequest has S3Authentication set,
  // and it is reset when read request is processed.
  private static final ThreadLocal<S3Authentication> S3_AUTH =
      new ThreadLocal<>();

  private static boolean securityEnabled = false;
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private OzoneBlockTokenSecretManager blockTokenMgr;
  private CertificateClient certClient;
  private String caCertPem = null;
  private List<String> caCertPemList = new ArrayList<>();
  private final Text omRpcAddressTxt;
  private OzoneConfiguration configuration;
  private RPC.Server omRpcServer;
  private GrpcOzoneManagerServer omS3gGrpcServer;
  private InetSocketAddress omRpcAddress;
  private String omId;

  private OMMetadataManager metadataManager;
  private OMMultiTenantManager multiTenantManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManagerImpl prefixManager;
  private UpgradeFinalizer<OzoneManager> upgradeFinalizer;

  /**
   * OM super user / admin list.
   */
  private final OzoneAdmins omAdmins;

  private final OMMetrics metrics;
  private final ProtocolMessageMetrics<ProtocolMessageEnum>
      omClientProtocolMetrics;
  private OzoneManagerHttpServer httpServer;
  private final OMStorage omStorage;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;
  private ObjectName omInfoBeanName;
  private Timer metricsTimer;
  private ScheduleOMMetricsWriteTask scheduleOMMetricsWriteTask;
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(OmMetricsInfo.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private final Runnable shutdownHook;
  private final File omMetaDir;
  private boolean isAclEnabled;
  private final boolean isSpnegoEnabled;
  private IAccessAuthorizer accessAuthorizer;
  private JvmPauseMonitor jvmPauseMonitor;
  private final SecurityConfig secConfig;
  private S3SecretManager s3SecretManager;
  private final boolean isOmGrpcServerEnabled;
  private volatile boolean isOmRpcServerRunning = false;
  private volatile boolean isOmGrpcServerRunning = false;
  private String omComponent;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;

  private boolean isRatisEnabled;
  private OzoneManagerRatisServer omRatisServer;
  private OzoneManagerSnapshotProvider omSnapshotProvider;
  private OMNodeDetails omNodeDetails;
  private Map<String, OMNodeDetails> peerNodesMap;
  private File omRatisSnapshotDir;
  private final RatisSnapshotInfo omRatisSnapshotInfo;
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();

  private KeyProviderCryptoExtension kmsProvider = null;
  private static String keyProviderUriKeyName =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
  private OMLayoutVersionManager versionManager;

  private boolean allowListAllVolumes;
  // Adding parameters needed for VolumeRequests here, so that during request
  // execution, we can get from ozoneManager.
  private long maxUserVolumeCount;

  private int minMultipartUploadPartSize = OzoneConsts.OM_MULTIPART_MIN_SIZE;

  private final ScmClient scmClient;
  private final long scmBlockSize;
  private final int preallocateBlocksMax;
  private final boolean grpcBlockTokenEnabled;
  private final boolean useRatisForReplication;
  private final String defaultBucketLayout;
  private ReplicationConfig defaultReplicationConfig;

  private boolean isS3MultiTenancyEnabled;

  private boolean isNativeAuthorizerEnabled;

  private ExitManager exitManager;

  private OzoneManagerPrepareState prepareState;

  private boolean isBootstrapping = false;
  private boolean isForcedBootstrapping = false;

  // Test flags
  private static boolean testReloadConfigFlag = false;
  private static boolean testSecureOmFlag = false;

  private final OzoneLockProvider ozoneLockProvider;

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

  private static final int MSECS_PER_MINUTE = 60 * 1000;

  private final boolean isSecurityEnabled;

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
    omId = omStorage.getOmId();

    versionManager = new OMLayoutVersionManager(omStorage.getLayoutVersion());
    upgradeFinalizer = new OMUpgradeFinalizer(versionManager);

    exitManager = new ExitManager();

    // In case of single OM Node Service there will be no OM Node ID
    // specified, set it to value from om storage
    if (this.omNodeDetails.getNodeId() == null) {
      this.omNodeDetails = OMHANodeDetails.getOMNodeDetailsForNonHA(conf,
          omNodeDetails.getServiceId(),
          omStorage.getOmId(), omNodeDetails.getRpcAddress(),
          omNodeDetails.getRatisPort());
    }

    loginOMUserIfSecurityEnabled(conf);
    setInstanceVariablesFromConf();
    this.maxUserVolumeCount = conf.getInt(OZONE_OM_USER_MAX_VOLUME,
        OZONE_OM_USER_MAX_VOLUME_DEFAULT);
    Preconditions.checkArgument(this.maxUserVolumeCount > 0,
        OZONE_OM_USER_MAX_VOLUME + " value should be greater than zero");

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
    this.useRatisForReplication = conf.getBoolean(
        DFS_CONTAINER_RATIS_ENABLED_KEY, DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    // TODO: This is a temporary check. Once fully implemented, all OM state
    //  change should go through Ratis - be it standalone (for non-HA) or
    //  replicated (for HA).
    isRatisEnabled = configuration.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_DEFAULT);

    this.defaultBucketLayout =
        configuration.getTrimmed(OZONE_DEFAULT_BUCKET_LAYOUT,
            OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT);

    if (!defaultBucketLayout.equals(
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name()) &&
        !defaultBucketLayout.equals(BucketLayout.OBJECT_STORE.name()) &&
        !defaultBucketLayout.equals(BucketLayout.LEGACY.name())
    ) {
      throw new ConfigurationException(
          defaultBucketLayout +
              " is not a valid default bucket layout. Supported values are " +
              BucketLayout.FILE_SYSTEM_OPTIMIZED + ", " +
              BucketLayout.OBJECT_STORE + ", " + BucketLayout.LEGACY + ".");
    }

    // Validates the default server-side replication configs.
    this.defaultReplicationConfig = getDefaultReplicationConfig();
    InetSocketAddress omNodeRpcAddr = omNodeDetails.getRpcAddress();
    omRpcAddressTxt = new Text(omNodeDetails.getRpcAddressString());

    scmContainerClient = getScmContainerClient(configuration);
    // verifies that the SCM info in the OM Version file is correct.
    scmBlockClient = getScmBlockClient(configuration);
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);
    this.ozoneLockProvider = new OzoneLockProvider(getKeyPathLockEnabled(),
        getEnableFileSystemPaths());

    // For testing purpose only, not hit scm from om as Hadoop UGI can't login
    // two principals in the same JVM.
    if (!testSecureOmFlag) {
      ScmInfo scmInfo = getScmInfo(configuration);
      if (!scmInfo.getClusterId().equals(omStorage.getClusterID())) {
        logVersionMismatch(conf, scmInfo);
        throw new OMException("SCM version info mismatch.",
            ResultCodes.SCM_VERSION_MISMATCH_ERROR);
      }
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
      if (omStorage.getOmCertSerialId() == null) {
        throw new RuntimeException("OzoneManager started in secure mode but " +
            "doesn't have SCM signed certificate.");
      }
      certClient = new OMCertificateClient(new SecurityConfig(conf),
          omStorage.getOmCertSerialId());
    }
    if (secConfig.isBlockTokenEnabled()) {
      blockTokenMgr = createBlockTokenSecretManager(configuration);
    }

    // Enable S3 multi-tenancy if config keys are set
    this.isS3MultiTenancyEnabled =
        OMMultiTenantManager.checkAndEnableMultiTenancy(this, conf);

    // Get admin list
    Collection<String> omAdminUsernames =
        getOzoneAdminsFromConfig(configuration);
    Collection<String> omAdminGroups =
        getOzoneAdminsGroupsFromConfig(configuration);
    omAdmins = new OzoneAdmins(omAdminUsernames, omAdminGroups);
    instantiateServices(false);

    // Create special volume s3v which is required for S3G.
    addS3GVolumeToDB();

    if (startupOption == StartupOption.BOOTSTRAP) {
      isBootstrapping = true;
    } else if (startupOption == StartupOption.FORCE_BOOTSTRAP) {
      isForcedBootstrapping = true;
    }

    this.omRatisSnapshotInfo = new RatisSnapshotInfo();

    initializeRatisDirs(conf);
    initializeRatisServer(isBootstrapping || isForcedBootstrapping);

    metrics = OMMetrics.create();
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
    shutdownHook = () -> {
      saveOmMetrics();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);

    if (isBootstrapping || isForcedBootstrapping) {
      omState = State.BOOTSTRAPPING;
    } else {
      omState = State.INITIALIZED;
    }
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
    this.allowListAllVolumes = configuration.getBoolean(
        OZONE_OM_VOLUME_LISTALL_ALLOWED,
        OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT);
  }

  /**
   * Constructs OM instance based on the configuration.
   *
   * @param conf OzoneConfiguration
   * @return OM instance
   * @throws IOException, AuthenticationException in case OM instance
   *                      creation fails.
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
    StringBuilder scmBlockAddressBuilder = new StringBuilder("");
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

    metadataManager = new OmMetadataManagerImpl(configuration);
    LOG.info("S3 Multi-Tenancy is {}",
        isS3MultiTenancyEnabled ? "enabled" : "disabled");
    if (isS3MultiTenancyEnabled) {
      multiTenantManager = new OMMultiTenantManagerImpl(this, configuration);
      OzoneAclUtils.setOMMultiTenantManager(multiTenantManager);
    }
    volumeManager = new VolumeManagerImpl(metadataManager, configuration);
    bucketManager = new BucketManagerImpl(metadataManager, getKmsProvider(),
        isRatisEnabled);
    if (secConfig.isSecurityEnabled() || testSecureOmFlag) {
      s3SecretManager = new S3SecretManagerImpl(configuration, metadataManager);
      delegationTokenMgr = createDelegationTokenSecretManager(configuration);
    }

    prefixManager = new PrefixManagerImpl(metadataManager, isRatisEnabled);
    keyManager = new KeyManagerImpl(this, scmClient, configuration,
        omStorage.getOmId());

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

    if (isAclEnabled) {
      accessAuthorizer = getACLAuthorizerInstance(configuration);
      if (accessAuthorizer instanceof OzoneNativeAuthorizer) {
        OzoneNativeAuthorizer authorizer =
            (OzoneNativeAuthorizer) accessAuthorizer;
        isNativeAuthorizerEnabled = true;
        authorizer.setVolumeManager(volumeManager);
        authorizer.setBucketManager(bucketManager);
        authorizer.setKeyManager(keyManager);
        authorizer.setPrefixManager(prefixManager);
        authorizer.setOzoneAdmins(omAdmins);
        authorizer.setAllowListAllVolumes(allowListAllVolumes);
      }
    } else {
      accessAuthorizer = null;
    }
  }

  /**
   * Return configuration value of
   * {@link OzoneConfigKeys#DFS_CONTAINER_RATIS_ENABLED_KEY}.
   */
  public boolean shouldUseRatis() {
    return useRatisForReplication;
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
        keyProviderUriKeyName);
    if (keyProvider == null) {
      return null;
    }
    KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
        .createKeyProviderCryptoExtension(keyProvider);
    return cryptoProvider;
  }

  /**
   * Returns an instance of {@link IAccessAuthorizer}.
   * Looks up the configuration to see if there is custom class specified.
   * Constructs the instance by passing the configuration directly to the
   * constructor to achieve thread safety using final fields.
   *
   * @param conf
   * @return IAccessAuthorizer
   */
  private IAccessAuthorizer getACLAuthorizerInstance(OzoneConfiguration conf) {
    Class<? extends IAccessAuthorizer> clazz = conf.getClass(
        OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizer.class,
        IAccessAuthorizer.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  /**
   * Class which schedule saving metrics to a file.
   */
  private class ScheduleOMMetricsWriteTask extends TimerTask {
    @Override
    public void run() {
      saveOmMetrics();
    }
  }

  private void saveOmMetrics() {
    try {
      boolean success;
      File parent = getTempMetricsStorageFile().getParentFile();
      if (!parent.exists()) {
        Files.createDirectories(parent.toPath());
      }
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(
              getTempMetricsStorageFile()), StandardCharsets.UTF_8))) {
        OmMetricsInfo metricsInfo = new OmMetricsInfo();
        metricsInfo.setNumKeys(metrics.getNumKeys());
        WRITER.writeValue(writer, metricsInfo);
        success = true;
      }

      if (success) {
        Files.move(getTempMetricsStorageFile().toPath(),
            getMetricsStorageFile().toPath(), StandardCopyOption
                .ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      }
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

    return new OzoneDelegationTokenSecretManager.Builder()
        .setConf(conf)
        .setTokenMaxLifetime(tokenMaxLifetime)
        .setTokenRenewInterval(tokenRenewInterval)
        .setTokenRemoverScanInterval(tokenRemoverScanInterval)
        .setService(omRpcAddressTxt)
        .setOzoneManager(this)
        .setS3SecretManager(s3SecretManager)
        .setCertificateClient(certClient)
        .setOmServiceId(omNodeDetails.getServiceId())
        .build();
  }

  private OzoneBlockTokenSecretManager createBlockTokenSecretManager(
      OzoneConfiguration conf) {

    long expiryTime = conf.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    // TODO: Pass OM cert serial ID.
    if (testSecureOmFlag) {
      return new OzoneBlockTokenSecretManager(secConfig, expiryTime, "1");
    }
    Objects.requireNonNull(certClient);
    return new OzoneBlockTokenSecretManager(secConfig, expiryTime,
        certClient.getCertificate().getSerialNumber().toString());
  }

  private void stopSecretManager() {
    if (blockTokenMgr != null) {
      LOG.info("Stopping OM block token manager.");
      try {
        blockTokenMgr.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop block token manager", e);
      }
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

  @VisibleForTesting
  public void startSecretManager() {
    try {
      certClient.assertValidKeysAndCertificate();
    } catch (OzoneSecurityException e) {
      LOG.error("Unable to read key pair for OM.", e);
      throw new UncheckedIOException(e);
    }
    if (secConfig.isBlockTokenEnabled() && blockTokenMgr != null) {
      try {
        LOG.info("Starting OM block token secret manager");
        blockTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting block token secret manager.", e);
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
  public void setCertClient(CertificateClient certClient) {
    // TODO: Initialize it in constructor with implementation for certClient.
    this.certClient = certClient;
  }

  /**
   * Login OM service user if security and Kerberos are enabled.
   *
   * @param conf
   * @throws IOException, AuthenticationException
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
   * @throws IOException
   */
  private static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) throws IOException {
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

    final int handlerCount = conf.getInt(OZONE_OM_HANDLER_COUNT_KEY,
        OZONE_OM_HANDLER_COUNT_DEFAULT);
    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omServerProtocol = new OzoneManagerProtocolServerSideTranslatorPB(
        this, omRatisServer, omClientProtocolMetrics, isRatisEnabled,
        getLastTrxnIndexForNonRatis());
    BlockingService omService =
        OzoneManagerService.newReflectiveBlockingService(omServerProtocol);

    OMInterServiceProtocolServerSideImpl omInterServerProtocol =
        new OMInterServiceProtocolServerSideImpl(this, omRatisServer,
            isRatisEnabled);
    BlockingService omInterService =
        OzoneManagerInterService.newReflectiveBlockingService(
            omInterServerProtocol);

    OMAdminProtocolServerSideImpl omMetadataServerProtocol =
        new OMAdminProtocolServerSideImpl(this);
    BlockingService omAdminService =
        OzoneManagerAdminService.newReflectiveBlockingService(
            omMetadataServerProtocol);

    return startRpcServer(configuration, omNodeRpcAddr, omService,
        omInterService, omAdminService, handlerCount);
  }

  /**
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param clientProtocolService RPC protocol for client communication
   *                              (OzoneManagerProtocolPB impl)
   * @param interOMProtocolService RPC protocol for inter OM communication
   *                               (OMInterServiceProtocolPB impl)
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, BlockingService clientProtocolService,
      BlockingService interOMProtocolService,
      BlockingService omAdminProtocolService,
      int handlerCount)
      throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(OzoneManagerProtocolPB.class)
        .setInstance(clientProtocolService)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(delegationTokenMgr)
        .build();

    HddsServerUtil.addPBProtocol(conf, OMInterServiceProtocolPB.class,
        interOMProtocolService, rpcServer);
    HddsServerUtil.addPBProtocol(conf, OMAdminProtocolPB.class,
        omAdminProtocolService, rpcServer);

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
  private GrpcOzoneManagerServer startGrpcServer(OzoneConfiguration conf)
          throws IOException {
    return new GrpcOzoneManagerServer(conf,
            this.omServerProtocol,
            this.delegationTokenMgr,
            this.certClient);
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
    if (securityEnabled) {
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
    OMHANodeDetails.loadOMHAConfig(conf);
    loginOMUserIfSecurityEnabled(conf);
    OMStorage omStorage = new OMStorage(conf);
    StorageState state = omStorage.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        ScmInfo scmInfo = getScmInfo(conf);
        String clusterId = scmInfo.getClusterId();
        String scmId = scmInfo.getScmId();
        if (clusterId == null || clusterId.isEmpty()) {
          throw new IOException("Invalid Cluster ID");
        }
        if (scmId == null || scmId.isEmpty()) {
          throw new IOException("Invalid SCM ID");
        }
        omStorage.setClusterId(clusterId);
        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          initializeSecurity(conf, omStorage, scmId);
        }
        omStorage.initialize();
        System.out.println(
            "OM initialization succeeded.Current cluster id for sd="
                + omStorage.getStorageDir() + ";cid=" + omStorage
                .getClusterID() + ";layoutVersion=" + omStorage
                .getLayoutVersion());

        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize OM version file", ioe);
        return false;
      }
    } else {
      if (OzoneSecurityUtil.isSecurityEnabled(conf) &&
          omStorage.getOmCertSerialId() == null) {
        ScmInfo scmInfo = HAUtils.getScmInfo(conf);
        String scmId = scmInfo.getScmId();
        if (scmId == null || scmId.isEmpty()) {
          throw new IOException("Invalid SCM ID");
        }
        LOG.info("OM storage is already initialized. Initializing security");
        initializeSecurity(conf, omStorage, scmId);
        omStorage.persistCurrentState();
      }
      System.out.println(
          "OM already initialized.Reusing existing cluster id for sd="
              + omStorage.getStorageDir() + ";cid=" + omStorage
              .getClusterID() + ";layoutVersion=" + omStorage
              .getLayoutVersion());
      return true;
    }
  }

  /**
   * Initializes secure OzoneManager.
   */
  @VisibleForTesting
  public static void initializeSecurity(OzoneConfiguration conf,
      OMStorage omStore, String scmId)
      throws IOException {
    LOG.info("Initializing secure OzoneManager.");

    CertificateClient certClient =
        new OMCertificateClient(new SecurityConfig(conf),
            omStore.getOmCertSerialId());
    CertificateClient.InitResponse response = certClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful.");
      break;
    case GETCERT:
      getSCMSignedCert(certClient, conf, omStore, scmId);
      LOG.info("Successfully stored SCM signed certificate.");
      break;
    case FAILURE:
      LOG.error("OM security initialization failed.");
      throw new RuntimeException("OM security initialization failed.");
    case RECOVER:
      LOG.error("OM security initialization failed. OM certificate is " +
          "missing.");
      throw new RuntimeException("OM security initialization failed.");
    default:
      LOG.error("OM security initialization failed. Init response: {}",
          response);
      throw new RuntimeException("OM security initialization failed.");
    }
  }

  private void initializeRatisDirs(OzoneConfiguration conf) throws IOException {
    if (isRatisEnabled) {
      // Create Ratis storage dir
      String omRatisDirectory =
          OzoneManagerRatisUtils.getOMRatisDirectory(conf);
      if (omRatisDirectory == null || omRatisDirectory.isEmpty()) {
        throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
            " must be defined.");
      }
      OmUtils.createOMDir(omRatisDirectory);

      // Create Ratis snapshot dir
      omRatisSnapshotDir = OmUtils.createOMDir(
          OzoneManagerRatisUtils.getOMRatisSnapshotDirectory(conf));

      // Before starting ratis server, check if previous installation has
      // snapshot directory in Ratis storage directory. if yes, move it to
      // new snapshot directory.

      File snapshotDir = new File(omRatisDirectory, OM_RATIS_SNAPSHOT_DIR);

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
            LOG.warn("Unknown file {} exists in ratis storage dir {}",
                ratisGroupDir, omRatisDir);
          }
        }
      }

      if (peerNodesMap != null && !peerNodesMap.isEmpty()) {
        this.omSnapshotProvider = new OzoneManagerSnapshotProvider(
            configuration, omRatisSnapshotDir, peerNodesMap);
      }
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
    return addr != null ? String.format("%s is listening at %s",
        description, addr.toString()) :
        String.format("%s not started", description);
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
  public OzoneManagerSnapshotProvider getOmSnapshotProvider() {
    return omSnapshotProvider;
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

  /**
   * Start service.
   */
  public void start() throws IOException {
    if (omState == State.BOOTSTRAPPING) {
      if (isBootstrapping) {
        // Check that all OM configs have been updated with the new OM info.
        checkConfigBeforeBootstrap();
      } else if (isForcedBootstrapping) {
        LOG.warn("Skipped checking whether existing OM configs have been " +
            "updated with this OM information as force bootstrap is called.");
      }
    }

    omClientProtocolMetrics.register();
    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    metadataManager.start(configuration);

    // Start Ratis services
    if (omRatisServer != null) {
      omRatisServer.start();
    }

    startSecretManagerIfNecessary();

    upgradeFinalizer.runPrefinalizeStateActions(omStorage, this);
    Integer layoutVersionInDB = getLayoutVersionInDB();
    if (layoutVersionInDB == null ||
        versionManager.getMetadataLayoutVersion() != layoutVersionInDB) {
      LOG.info("Version File has different layout " +
              "version ({}) than OM DB ({}). That is expected if this " +
              "OM has never been finalized to a newer layout version.",
          versionManager.getMetadataLayoutVersion(), layoutVersionInDB);
    }

    // Perform this to make it work with old clients.
    if (certClient != null) {
      caCertPem =
          CertificateCodec.getPEMEncodedString(certClient.getCACertificate());
      caCertPemList = HAUtils.buildCAList(certClient, configuration);
    }

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

    startJVMPauseMonitor();
    setStartTime();

    if (omState == State.BOOTSTRAPPING) {
      bootstrap(omNodeDetails);
    }

    omState = State.RUNNING;
  }

  /**
   * Restarts the service. This method re-initializes the rpc server.
   */
  public void restart() throws IOException {
    setInstanceVariablesFromConf();

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    instantiateServices(false);

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
    startJVMPauseMonitor();
    setStartTime();
    omState = State.RUNNING;
  }

  private void checkConfigBeforeBootstrap() throws IOException {
    List<OMNodeDetails> omsWihtoutNewConfig = new ArrayList<>();
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
        omsWihtoutNewConfig.add(remoteNodeDetails);
      }
    }
    if (!omsWihtoutNewConfig.isEmpty()) {
      String errorMsg = OmUtils.getOMAddressListPrintString(omsWihtoutNewConfig)
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
      throw new IOException("Remote OM " + remoteNodeId + " already contains " +
          "bootstrapping OM(" + getOMNodeId() + ") as part of its Raft group " +
          "peers.");
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
    if (isRatisEnabled) {
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
    } else {
      throw new IOException("OzoneManager can be bootstrapped only when ratis" +
          " is enabled and there is atleast one OzoneManager to bootstrap" +
          " from.");
    }
  }

  /**
   * When OMStateMachine receives a configuration change update, it calls
   * this function to update the peers list, if required. The configuration
   * change could be to add or to remove an OM from the ring.
   */
  public void updatePeerList(List<String> newPeers) {
    List<String> currentPeers = omRatisServer.getPeerIds();

    // NodeIds present in new node list and not in current peer list are the
    // bootstapped OMs and should be added to the peer list
    List<String> bootstrappedOMs = new ArrayList<>();
    bootstrappedOMs.addAll(newPeers);
    bootstrappedOMs.removeAll(currentPeers);

    // NodeIds present in current peer list but not in new node list are the
    // decommissioned OMs and should be removed from the peer list
    List<String> decommissionedOMs = new ArrayList<>();
    decommissionedOMs.addAll(currentPeers);
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
        // request. Shut it down.
        String errorMsg = "Shutting down as OM has been decommissioned.";
        LOG.error("Fatal Error: {}", errorMsg);
        exitManager.forceExit(1, errorMsg, LOG);
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
          // configuration also, throw an exception
          throw new IOException("There is no OM configuration for node ID "
              + newOMNodeId + " in ozone-site.xml.");
        }
      }
    } catch (IOException e) {
      LOG.error("{}: Couldn't add OM {} to peer list.", getOMNodeId(),
          newOMNodeId);
      exitManager.exitSystem(1, e.getLocalizedMessage(), e, LOG);
    }

    if (omSnapshotProvider == null) {
      omSnapshotProvider = new OzoneManagerSnapshotProvider(
          configuration, omRatisSnapshotDir, peerNodesMap);
    } else {
      omSnapshotProvider.addNewPeerNode(newOMNodeDetails);
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

    omSnapshotProvider.removeDecommissionedPeerNode(decommNodeId);
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
   * @param conf
   * @throws IOException
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
          new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws IOException {
              return new TrashOzoneFileSystem(i);
            }
          });
      this.emptier = new Thread(new OzoneTrash(fs, conf, this).
          getEmptier(), "Trash Emptier");
      this.emptier.setDaemon(true);
      this.emptier.start();
    }
  }

  /**
   * Creates a new instance of gRPC OzoneManagerServiceGrpc transport server
   * for serving s3g OmRequests.  If an earlier instance is already running
   * then returns the same.
   */
  private GrpcOzoneManagerServer getOmS3gGrpcServer(OzoneConfiguration conf)
      throws IOException {
    if (isOmGrpcServerRunning) {
      return omS3gGrpcServer;
    }
    return startGrpcServer(configuration);
  }

  /**
   * Creates an instance of ratis server.
   */
  /**
   * Creates an instance of ratis server.
   * @param shouldBootstrap If OM is started in Bootstrap mode, then Ratis
   *                        server will be initialized without adding self to
   *                        Ratis group
   * @throws IOException
   */
  private void initializeRatisServer(boolean shouldBootstrap)
      throws IOException {
    if (isRatisEnabled) {
      if (omRatisServer == null) {
        // This needs to be done before initializing Ratis.
        RatisDropwizardExports.
            registerRatisMetricReporters(ratisMetricsMap);
        omRatisServer = OzoneManagerRatisServer.newOMRatisServer(
            configuration, this, omNodeDetails, peerNodesMap,
            secConfig, certClient, shouldBootstrap);
      }
      LOG.info("OzoneManager Ratis server initialized at port {}",
          omRatisServer.getServerPort());
    } else {
      omRatisServer = null;
    }
  }

  public long getObjectIdFromTxId(long trxnId) {
    return OmUtils.getObjectIdFromTxId(metadataManager.getOmEpoch(),
        trxnId);
  }

  @VisibleForTesting
  long getLastTrxnIndexForNonRatis() throws IOException {
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(metadataManager);
    // If the OMTransactionInfo does not exist in DB or if the term is not -1
    // (corresponding to non-Ratis cluster), return 0 so that new incoming
    // requests can have transaction index starting from 1.
    if (transactionInfo == null || transactionInfo.getTerm() != -1) {
      return 0;
    }
    // If there exists a last transaction index in DB, the new incoming
    // requests in non-Ratis cluster must have transaction index
    // incrementally increasing from the stored transaction index onwards.
    return transactionInfo.getTransactionIndex();
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

  public RatisSnapshotInfo getSnapshotInfo() {
    return omRatisSnapshotInfo;
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
  public void stop() {
    LOG.info("{}: Stopping Ozone Manager", omNodeDetails.getOMPrintInfo());
    if (isStopped()) {
      return;
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
      if (!isRatisEnabled) {
        omServerProtocol.stop();
      }
      if (omRatisServer != null) {
        omRatisServer.stop();
        omRatisServer = null;
      }
      isOmRpcServerRunning = false;
      if (isOmGrpcServerEnabled) {
        isOmGrpcServerRunning = false;
      }
      keyManager.stop();
      stopSecretManager();
      if (httpServer != null) {
        httpServer.stop();
      }
      if (multiTenantManager != null) {
        multiTenantManager.stop();
      }
      stopTrashEmptier();
      metadataManager.stop();
      metrics.unRegister();
      omClientProtocolMetrics.unregister();
      unregisterMXBean();
      if (jvmPauseMonitor != null) {
        jvmPauseMonitor.stop();
      }
      if (omSnapshotProvider != null) {
        omSnapshotProvider.stop();
      }
    } catch (Exception e) {
      LOG.error("OzoneManager stop failed.", e);
    }
  }

  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(1, message, LOG);
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
      boolean running = delegationTokenMgr.isRunning()
          && blockTokenMgr.isRunning();
      if (!running) {
        startSecretManager();
      }
    }
  }

  /**
   * Get SCM signed certificate and store it using certificate client.
   */
  private static void getSCMSignedCert(CertificateClient client,
      OzoneConfiguration config, OMStorage omStore, String scmId)
      throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();
    KeyPair keyPair = new KeyPair(client.getPublicKey(),
        client.getPrivateKey());
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
    String subject;
    if (builder.hasDnsName()) {
      subject = UserGroupInformation.getCurrentUser().getShortUserName()
          + "@" + hostname;
    } else {
      // With only IP in alt.name, certificate validation would fail if subject
      // isn't a hostname either, so omit username.
      subject = hostname;
    }

    builder.setCA(false)
        .setKey(keyPair)
        .setConfiguration(config)
        .setScmID(scmId)
        .setClusterID(omStore.getClusterID())
        .setSubject(subject);

    OMHANodeDetails haOMHANodeDetails = OMHANodeDetails.loadOMHAConfig(config);
    String serviceName =
        haOMHANodeDetails.getLocalNodeDetails().getServiceId();
    if (!StringUtils.isEmpty(serviceName)) {
      builder.addServiceName(serviceName);
    }

    LOG.info("Creating csr for OM->dns:{},ip:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, ip, scmId, omStore.getClusterID(), subject);

    HddsProtos.OzoneManagerDetailsProto.Builder omDetailsProtoBuilder =
        HddsProtos.OzoneManagerDetailsProto.newBuilder()
            .setHostName(hostname)
            .setIpAddress(ip)
            .setUuid(omStore.getOmId())
            .addPorts(HddsProtos.Port.newBuilder()
                .setName(RPC_PORT)
                .setValue(port)
                .build());

    PKCS10CertificationRequest csr = builder.build();
    HddsProtos.OzoneManagerDetailsProto omDetailsProto =
        omDetailsProtoBuilder.build();
    LOG.info("OzoneManager ports added:{}", omDetailsProto.getPortsList());
    SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
        HddsServerUtil.getScmSecurityClientWithFixedDuration(config);

    SCMGetCertResponseProto response = secureScmClient.
        getOMCertChain(omDetailsProto, getEncodedString(csr));
    String pemEncodedCert = response.getX509Certificate();

    try {

      // Store SCM CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        client.storeCertificate(pemEncodedRootCert, true, true);
        client.storeCertificate(pemEncodedCert, true);

        // Store Root CA certificate if available.
        if (response.hasX509RootCACertificate()) {
          client.storeRootCACertificate(response.getX509RootCACertificate(),
              true);
        }

        // Persist om cert serial id.
        omStore.setOmCertSerialId(CertificateCodec.
            getX509Certificate(pemEncodedCert).getSerialNumber().toString());
      } else {
        throw new RuntimeException("Unable to retrieve OM certificate " +
            "chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }

  }

  /**
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }

  /**
   * Returns authentication method used to establish the connection.
   *
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
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

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Get delegation token from OzoneManager.
   *
   * @param renewer Renewer information
   * @return delegationToken DelegationToken signed by OzoneManager
   * @throws IOException on error
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws OMException {
    Token<OzoneTokenIdentifier> token;
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
   * @throws InvalidToken if {@code token} is invalid
   * @throws IOException  on other errors
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
      } catch (IOException exe) {
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
   * @throws IOException on error
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    OzoneTokenIdentifier id = null;
    try {
      String canceller = getRemoteUser().getUserName();
      id = delegationTokenMgr.cancelToken(token, canceller);
      LOG.trace("Delegation token cancelled for dt: {}", id);
    } catch (OMException oex) {
      throw oex;
    } catch (IOException ex) {
      LOG.error("Delegation token cancellation failed for dt id: {}, cause: {}",
          id, ex.getMessage());
      throw new OMException("Delegation token renewal failed for dt: " + token,
          ex, TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Checks if current caller has acl permissions.
   *
   * @param resType - Type of ozone resource. Ex volume, bucket.
   * @param store   - Store type. i.e Ozone, S3.
   * @param acl     - type of access to be checked.
   * @param vol     - name of volume
   * @param bucket  - bucket name
   * @param key     - key
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied.
   */
  private void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, String vol, String bucket, String key)
      throws IOException {
    UserGroupInformation user;
    if (getS3Auth() != null) {
      String principal =
          OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId());
      user = UserGroupInformation.createRemoteUser(principal);
    } else {
      user = ProtobufRpcEngine.Server.getRemoteUser();
    }

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    String volumeOwner = getVolumeOwner(vol, acl, resType);
    String bucketOwner = getBucketOwner(vol, bucket, acl, resType);

    OzoneAclUtils.checkAllAcls(this, resType, store, acl,
        vol, bucket, key, volumeOwner, bucketOwner,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
        remoteIp != null ? remoteIp.getHostName() : omRpcAddress.getHostName());
  }

  private boolean isOwner(UserGroupInformation callerUgi, String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getUserName().equals(ownerName) ||
            callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
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
    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
        VOLUME_LOCK, volume);
    String dbVolumeKey = metadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs = null;
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

    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
            BUCKET_LOCK, volume, bucket);
    String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = null;
    try {
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

    return checkAcls(obj, context, throwIfPermissionDenied);
  }

  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  public boolean checkAcls(OzoneObj obj, RequestContext context,
                           boolean throwIfPermissionDenied)
      throws OMException {

    if (!accessAuthorizer.checkAccess(obj, context)) {
      if (throwIfPermissionDenied) {
        String volumeName = obj.getVolumeName() != null ?
                "Volume:" + obj.getVolumeName() + " " : "";
        String bucketName = obj.getBucketName() != null ?
                "Bucket:" + obj.getBucketName() + " " : "";
        String keyName = obj.getKeyName() != null ?
                "Key:" + obj.getKeyName() : "";
        LOG.warn("User {} doesn't have {} permission to access {} {}{}{}",
            context.getClientUgi().getUserName(), context.getAclRights(),
            obj.getResourceType(), volumeName, bucketName, keyName);
        throw new OMException("User " + context.getClientUgi().getUserName() +
            " doesn't have " + context.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            volumeName  + bucketName + keyName, ResultCodes.PERMISSION_DENIED);
      }
      return false;
    } else {
      return true;
    }
  }



  /**
   * Return true if Ozone acl's are enabled, else false.
   *
   * @return boolean
   */
  public boolean getAclsEnabled() {
    return isAclEnabled;
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
   * @throws IOException
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.READ, volume,
          null, null);
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    try {
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
   * @throws IOException
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
   * @throws IOException
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
      if (!allowListAllVolumes) {
        // Only admin can list all volumes when disallowed in config
        if (isAclEnabled) {
          checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.LIST,
              OzoneConsts.OZONE_ROOT, null, null);
        }
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
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startKey, String prefix, int maxNumOfBuckets)
      throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.LIST, volumeName,
          null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.PREFIX, prefix);
    auditMap.put(OzoneConsts.MAX_NUM_OF_BUCKETS,
        String.valueOf(maxNumOfBuckets));
    try {
      metrics.incNumBucketLists();
      return bucketManager.listBuckets(volumeName,
          startKey, prefix, maxNumOfBuckets);
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
   * @throws IOException
   */
  @Override
  public OmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.READ, volume,
          bucket, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.BUCKET, bucket);
    try {
      metrics.incNumBucketInfos();
      final OmBucketInfo bucketInfo =
          bucketManager.getBucketInfo(volume, bucket);
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
   * Lookup a key.
   *
   * @param args - attributes of the key.
   * @return OmKeyInfo - the info about the requested key.
   * @throws IOException
   */
  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumKeyLookups();
      return keyManager.lookupKey(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumKeyLookupFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
            auditMap));
      }
    }
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {

    ResolvedBucket bucket = resolveBucketLink(Pair.of(volumeName, bucketName));

    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.LIST,
          bucket.realVolume(), bucket.realBucket(), keyPrefix);
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.KEY_PREFIX, keyPrefix);

    try {
      metrics.incNumKeyLists();
      return keyManager.listKeys(bucket.realVolume(), bucket.realBucket(),
          startKey, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumKeyListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_KEYS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_KEYS,
            auditMap));
      }
    }
  }

  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName,
      String bucketName, String startKeyName, String keyPrefix, int maxKeys)
      throws IOException {

    // bucket links not supported

    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.LIST,
          volumeName, bucketName, keyPrefix);
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.START_KEY, startKeyName);
    auditMap.put(OzoneConsts.KEY_PREFIX, keyPrefix);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));

    try {
      metrics.incNumTrashKeyLists();
      return keyManager.listTrash(volumeName, bucketName,
          startKeyName, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumTrashKeyListFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_TRASH,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_TRASH,
            auditMap));
      }
    }
  }

  private Map<String, String> buildAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }

  public AuditLogger getAuditLogger() {
    return AUDIT;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable)
        .build();
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

  private static String getClientAddress() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
  }

  @Override
  public String getRpcPort() {
    return "" + omRpcAddress.getPort();
  }

  @Override
  public String getRatisLogDirectory() {
    return  OzoneManagerRatisUtils.getOMRatisDirectory(configuration);
  }

  @Override
  public String getRocksDbDirectory() {
    return String.valueOf(OMStorage.getOmDbDir(configuration));
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

    if (isRatisEnabled) {
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
    return new ServiceInfoEx(getServiceList(), caCertPem, caCertPemList);
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
      final Thread t = new Thread(bgSync::triggerRangerSyncOnce);
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
  public TenantStateList listTenant() throws IOException {

    metrics.incNumTenantLists();

    final UserGroupInformation ugi = getRemoteUser();
    if (!isAdmin(ugi)) {
      final OMException omEx = new OMException(
          "Only Ozone admins are allowed to list tenants.", PERMISSION_DENIED);
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
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

    boolean lockAcquired =
        metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName);
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
          multiTenantManager.getTenantForAccessID(accessId) : Optional.absent();

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

        boolean acquiredVolumeLock =
            getMetadataManager().getLock().acquireReadLock(
                VOLUME_LOCK, s3Volume);

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

    // getVolumeInfo() performs acl checks and checks volume existence.
    final S3VolumeContext.Builder s3VolumeContext = S3VolumeContext.newBuilder()
        .setOmVolumeArgs(getVolumeInfo(s3Volume))
        .setUserPrincipal(userPrincipal);

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
      String bucketName, String prefix) throws IOException {

    ResolvedBucket bucket = resolveBucketLink(Pair.of(volumeName, bucketName));

    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.PREFIX, prefix);

    metrics.incNumListMultipartUploads();
    try {
      OmMultipartUploadList omMultipartUploadList =
          keyManager.listMultipartUploads(bucket.realVolume(),
              bucket.realBucket(), prefix);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction
          .LIST_MULTIPART_UPLOADS, auditMap));
      return omMultipartUploadList;

    } catch (IOException ex) {
      metrics.incNumListMultipartUploadFails();
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction
          .LIST_MULTIPART_UPLOADS, auditMap, ex));
      throw ex;
    }

  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumGetFileStatus();
      return keyManager.getFileStatus(args, getClientAddress());
    } catch (IOException ex) {
      metrics.incNumGetFileStatusFails();
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_FILE_STATUS, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_FILE_STATUS, auditMap));
      }
    }
  }

  private ResourceType getResourceType(OmKeyArgs args) {
    if (args.getKeyName() == null || args.getKeyName().length() == 0) {
      return ResourceType.BUCKET;
    }
    return ResourceType.KEY;
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumLookupFile();
      return keyManager.lookupFile(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumLookupFileFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LOOKUP_FILE,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LOOKUP_FILE, auditMap));
      }
    }
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries)
      throws IOException {
    return listStatus(args, recursive, startKey, numEntries, false);
  }

  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {

    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumListStatus();
      return keyManager.listStatus(args, recursive, startKey, numEntries,
              getClientAddress(), allowPartialPrefixes);
    } catch (Exception ex) {
      metrics.incNumListStatusFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_STATUS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LIST_STATUS, auditMap));
      }
    }
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.READ_ACL,
            obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
      }
      metrics.incNumGetAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.getAcl(obj);
      case BUCKET:
        return bucketManager.getAcl(obj);
      case KEY:
        return keyManager.getAcl(obj);
      case PREFIX:
        return prefixManager.getAcl(obj);

      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_ACL, obj.toAuditMap(), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_ACL, obj.toAuditMap()));
      }
    }
  }

  /**
   * Download and install latest checkpoint from leader OM.
   *
   * @param leaderId peerNodeID of the leader OM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  public TermIndex installSnapshotFromLeader(String leaderId) {
    if (omSnapshotProvider == null) {
      LOG.error("OM Snapshot Provider is not configured as there are no peer " +
          "nodes.");
      return null;
    }

    DBCheckpoint omDBCheckpoint = getDBCheckpointFromLeader(leaderId);
    LOG.info("Downloaded checkpoint from Leader {} to the location {}",
        leaderId, omDBCheckpoint.getCheckpointLocation());

    TermIndex termIndex = null;
    try {
      termIndex = installCheckpoint(leaderId, omDBCheckpoint);
    } catch (Exception ex) {
      LOG.error("Failed to install snapshot from Leader OM.", ex);
    }
    return termIndex;
  }

  /**
   * Install checkpoint. If the checkpoints snapshot index is greater than
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
        dbBackup = replaceOMDBWithCheckpoint(lastAppliedIndex, oldDBLocation,
            checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", leaderId, term, lastAppliedIndex,
            Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
            " DB with downloaded checkpoint. Reloading old OM state.", e);
      }
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at TermIndex {} " +
          "and checkpoint has lower TermIndex {}. Reloading old state of OM.",
          termIndex, checkpointTrxnInfo.getTermIndex());
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      if (oldOmMetadataManagerStopped) {
        time = Time.monotonicNow();
        reloadOMState(lastAppliedIndex, term);
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
      LOG.error("Failed to delete the backup of the original DB {}", dbBackup);
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


  /**
   * Download the latest OM DB checkpoint from the leader OM.
   *
   * @param leaderId OMNodeID of the leader OM node.
   * @return latest DB checkpoint from leader OM.
   */
  private DBCheckpoint getDBCheckpointFromLeader(String leaderId) {
    LOG.info("Downloading checkpoint from leader OM {} and reloading state " +
        "from the checkpoint.", leaderId);

    try {
      return omSnapshotProvider.getOzoneManagerDBSnapshot(leaderId);
    } catch (IOException e) {
      LOG.error("Failed to download checkpoint from OM leader {}", leaderId, e);
    }
    return null;
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
   * @throws Exception
   */
  File replaceOMDBWithCheckpoint(long lastAppliedIndex, File oldDB,
      Path checkpointPath) throws IOException {

    // Take a backup of the current DB
    String dbBackupName = OzoneConsts.OM_DB_BACKUP_PREFIX +
        lastAppliedIndex + "_" + System.currentTimeMillis();
    File dbDir = oldDB.getParentFile();
    File dbBackup = new File(dbDir, dbBackupName);

    try {
      Files.move(oldDB.toPath(), dbBackup.toPath());
    } catch (IOException e) {
      LOG.error("Failed to create a backup of the current DB. Aborting " +
          "snapshot installation.");
      throw e;
    }

    // Move the new DB checkpoint into the om metadata dir
    Path markerFile = new File(dbDir, DB_TRANSIENT_MARKER).toPath();
    try {
      // Create a Transient Marker file. This file will be deleted if the
      // checkpoint DB is successfully moved to the old DB location or if the
      // old DB backup is reset to its location. If not, then the OM DB is in
      // an inconsistent state and this marker file will fail OM from
      // starting up.
      Files.createFile(markerFile);
      FileUtils.moveDirectory(checkpointPath, oldDB.toPath());
      Files.deleteIfExists(markerFile);
    } catch (IOException e) {
      LOG.error("Failed to move downloaded DB checkpoint {} to metadata " +
              "directory {}. Resetting to original DB.", checkpointPath,
          oldDB.toPath());
      try {
        Files.move(dbBackup.toPath(), oldDB.toPath());
        Files.deleteIfExists(markerFile);
      } catch (IOException ex) {
        String errorMsg = "Failed to reset to original DB. OM is in an " +
            "inconsistent state.";
        exitManager.exitSystem(1, errorMsg, ex, LOG);
      }
      throw e;
    }
    return dbBackup;
  }

  /**
   * Re-instantiate MetadataManager with new DB checkpoint.
   * All the classes which use/ store MetadataManager should also be updated
   * with the new MetadataManager instance.
   */
  void reloadOMState(long newSnapshotIndex, long newSnapshotTermIndex)
      throws IOException {

    instantiateServices(true);

    // Restart required services
    metadataManager.start(configuration);
    keyManager.start(configuration);
    startSecretManagerIfNecessary();
    startTrashEmptier(configuration);

    // Set metrics and start metrics back ground thread
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

    // Delete the omMetrics file if it exists and save the a new metrics file
    // with new data
    Files.deleteIfExists(getMetricsStorageFile().toPath());
    saveOmMetrics();

    // Update OM snapshot index with the new snapshot index (from the new OM
    // DB state).
    omRatisSnapshotInfo.updateTermIndex(newSnapshotTermIndex, newSnapshotIndex);
  }

  public static Logger getLogger() {
    return LOG;
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  @VisibleForTesting
  public void setConfiguration(OzoneConfiguration conf) {
    this.configuration = conf;
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
    return maxUserVolumeCount;
  }
  /**
   * Return true, if the current OM node is leader and in ready state to
   * process the requests.
   *
   * If ratis is not enabled, then it always returns true.
   * @return
   */
  public boolean isLeaderReady() {
    return isRatisEnabled ?
        omRatisServer.checkLeaderStatus() == LEADER_AND_READY : true;
  }

  /**
   * Check the leader status.
   *
   * @return null                       leader is ready
   *         OMLeaderNotReadyException  leader is not ready
   *         OMNotLeaderException       not leader
   */
  public void checkLeaderStatus() throws OMNotLeaderException,
      OMLeaderNotReadyException {
    OzoneManagerRatisServer.RaftServerStatus raftServerStatus =
        omRatisServer.checkLeaderStatus();
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    switch (raftServerStatus) {
    case LEADER_AND_READY: return;
    case LEADER_AND_NOT_READY:
      throw new OMLeaderNotReadyException(
        raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");
    case NOT_LEADER:
      // TODO: Set suggest leaderID. Right now, client is not using suggest
      // leaderID. Need to fix this.
      throw new OMNotLeaderException(raftPeerId);
    default: throw new IllegalStateException(
        "Unknown Ratis Server state: " + raftServerStatus);
    }
  }

  /**
   * Return if Ratis is enabled or not.
   *
   * @return
   */
  public boolean isRatisEnabled() {
    return isRatisEnabled;
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
      throws SequenceNumberNotFoundException {
    long limitCount = Long.MAX_VALUE;
    if (dbUpdatesRequest.hasLimitCount()) {
      limitCount = dbUpdatesRequest.getLimitCount();
    }
    DBUpdatesWrapper updatesSince = metadataManager.getStore()
        .getUpdatesSince(dbUpdatesRequest.getSequenceNumber(), limitCount);
    DBUpdates dbUpdates = new DBUpdates(updatesSince.getData());
    dbUpdates.setCurrentSequenceNumber(updatesSince.getCurrentSequenceNumber());
    dbUpdates.setLatestSequenceNumber(updatesSince.getLatestSequenceNumber());
    return dbUpdates;
  }

  public OzoneDelegationTokenSecretManager getDelegationTokenMgr() {
    return delegationTokenMgr;
  }

  /**
   * Return list of OzoneAdministrators from config.
   */
  Collection<String> getOzoneAdminsFromConfig(OzoneConfiguration conf)
      throws IOException {
    Collection<String> ozAdmins =
        conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!ozAdmins.contains(omSPN)) {
      ozAdmins.add(omSPN);
    }
    return ozAdmins;
  }

  Collection<String> getOzoneAdminsGroupsFromConfig(OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS_GROUPS);
  }

  /**
   * Return the list of Ozone administrators in effect.
   */
  Collection<String> getOmAdminUsernames() {
    return omAdmins.getAdminUsernames();
  }

  public Collection<String> getOmAdminGroups() {
    return omAdmins.getAdminGroups();
  }

  /**
   * Return true if a UserGroupInformation is OM admin, false otherwise.
   * @param callerUgi Caller UserGroupInformation
   */
  public boolean isAdmin(UserGroupInformation callerUgi) {
    return callerUgi != null && omAdmins.isAdmin(callerUgi);
  }

  /**
   * Returns true if OzoneNativeAuthorizer is enabled and false if otherwise.
   *
   * @return if native authorizer is enabled.
   */
  public boolean isNativeAuthorizerEnabled() {
    return isNativeAuthorizerEnabled;
  }

  @VisibleForTesting
  public boolean isRunning() {
    return omState == State.RUNNING;
  }

  private void startJVMPauseMonitor() {
    // Start jvm monitor
    jvmPauseMonitor = new JvmPauseMonitor();
    jvmPauseMonitor.init(configuration);
    jvmPauseMonitor.start();
  }

  public ResolvedBucket resolveBucketLink(KeyArgs args,
      OMClientRequest omClientRequest) throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()), omClientRequest);
  }

  public ResolvedBucket resolveBucketLink(OmKeyArgs args)
      throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()));
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested,
      OMClientRequest omClientRequest)
      throws IOException {
    Pair<String, String> resolved;
    if (isAclEnabled) {
      resolved = resolveBucketLink(requested, new HashSet<>(),
              omClientRequest.createUGI(), omClientRequest.getRemoteAddress(),
              omClientRequest.getHostName());
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null);
    }
    return new ResolvedBucket(requested, resolved);
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested)
      throws IOException {

    Pair<String, String> resolved;
    if (isAclEnabled) {
      UserGroupInformation ugi = getRemoteUser();
      if (getS3Auth() != null) {
        ugi = UserGroupInformation.createRemoteUser(
            OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId()));
      }
      InetAddress remoteIp = Server.getRemoteIp();
      resolved = resolveBucketLink(requested, new HashSet<>(),
          ugi,
          remoteIp,
          remoteIp != null ? remoteIp.getHostName() :
              omRpcAddress.getHostName());
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null);
    }
    return new ResolvedBucket(requested, resolved);
  }

  /**
   * Resolves bucket symlinks. Read permission is required for following links.
   *
   * @param volumeAndBucket the bucket to be resolved (if it is a link)
   * @param visited collects link buckets visited during the resolution to
   *   avoid infinite loops
   * @param {@link UserGroupInformation}
   * @param remoteAddress
   * @param hostName
   * @return bucket location possibly updated with its actual volume and bucket
   *   after following bucket links
   * @throws IOException (most likely OMException) if ACL check fails, bucket is
   *   not found, loop is detected in the links, etc.
   */
  private Pair<String, String> resolveBucketLink(
      Pair<String, String> volumeAndBucket,
      Set<Pair<String, String>> visited,
      UserGroupInformation userGroupInformation,
      InetAddress remoteAddress,
      String hostName) throws IOException {

    String volumeName = volumeAndBucket.getLeft();
    String bucketName = volumeAndBucket.getRight();
    OmBucketInfo info = bucketManager.getBucketInfo(volumeName, bucketName);
    if (!info.isLink()) {
      return volumeAndBucket;
    }

    if (!visited.add(volumeAndBucket)) {
      throw new OMException("Detected loop in bucket links",
          DETECTED_LOOP_IN_BUCKET_LINKS);
    }

    if (isAclEnabled) {
      final ACLType type = ACLType.READ;
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, type,
          volumeName, bucketName, null, userGroupInformation,
          remoteAddress, hostName, true,
          getVolumeOwner(volumeName, type, ResourceType.BUCKET));
    }

    return resolveBucketLink(
        Pair.of(info.getSourceVolume(), info.getSourceBucket()),
        visited, userGroupInformation, remoteAddress, hostName);
  }

  @VisibleForTesting
  public void setExitManagerForTesting(ExitManager exitManagerForTesting) {
    exitManager = exitManagerForTesting;
  }

  public boolean getEnableFileSystemPaths() {
    return configuration.getBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);
  }

  public boolean getKeyPathLockEnabled() {
    return configuration.getBoolean(OZONE_OM_KEY_PATH_LOCK_ENABLED,
        OZONE_OM_KEY_PATH_LOCK_ENABLED_DEFAULT);
  }

  public OzoneLockProvider getOzoneLockProvider() {
    return this.ozoneLockProvider;
  }

  public ReplicationConfig getDefaultReplicationConfig() {
    if (this.defaultReplicationConfig != null) {
      return this.defaultReplicationConfig;
    }

    final String replication = configuration.getTrimmed(
        OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT);
    final String type = configuration.getTrimmed(
        OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        OZONE_SERVER_DEFAULT_REPLICATION_TYPE_DEFAULT);
    return ReplicationConfig.parse(ReplicationType.valueOf(type),
        replication, configuration);
  }

  public String getOMDefaultBucketLayout() {
    return this.defaultBucketLayout;
  }

  /**
   * Create volume which is required for S3Gateway operations.
   * @throws IOException
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
          new CacheValue<>(Optional.of(omVolumeArgs), transactionID));
      metadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(dbUserKey),
          new CacheValue<>(Optional.of(userVolumeInfo), transactionID));
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
    listOfAcls.add(new OzoneAcl(ACLIdentityType.USER,
        userName, ACLType.ALL, ACCESS));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(userName).getGroupNames());

    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(ACLIdentityType.GROUP, group, ACLType.ALL, ACCESS)));

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
  public EchoRPCResponse echoRPCReq(byte[] payloadReq,
                                    int payloadSizeResp)
          throws IOException {
    return null;
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
}
