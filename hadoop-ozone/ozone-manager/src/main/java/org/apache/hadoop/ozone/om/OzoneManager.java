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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PrivilegedExceptionAction;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
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
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.ozone.om.ha.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.common.ha.ratis.RatisSnapshotInfo;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.snapshot.OzoneManagerSnapshotProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneSecurityException;
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
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_METRICS_TEMP_FILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.RPC_PORT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneManagerService.newReflectiveBlockingService;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
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
    implements OzoneManagerProtocol, OMMXBean, Auditor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final String OM_DAEMON = "om";

  private static boolean securityEnabled = false;
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private OzoneBlockTokenSecretManager blockTokenMgr;
  private CertificateClient certClient;
  private String caCertPem = null;
  private List<String> caCertPemList = new ArrayList<>();
  private static boolean testSecureOmFlag = false;
  private final Text omRpcAddressTxt;
  private final OzoneConfiguration configuration;
  private RPC.Server omRpcServer;
  private InetSocketAddress omRpcAddress;
  private String omId;

  private OMMetadataManager metadataManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManagerImpl prefixManager;

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
  private final boolean isAclEnabled;
  private IAccessAuthorizer accessAuthorizer;
  private JvmPauseMonitor jvmPauseMonitor;
  private final SecurityConfig secConfig;
  private S3SecretManager s3SecretManager;
  private volatile boolean isOmRpcServerRunning = false;
  private String omComponent;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;

  private boolean isRatisEnabled;
  private OzoneManagerRatisServer omRatisServer;
  private OzoneManagerSnapshotProvider omSnapshotProvider;
  private OMNodeDetails omNodeDetails;
  private List<OMNodeDetails> peerNodes;
  private File omRatisSnapshotDir;
  private final RatisSnapshotInfo omRatisSnapshotInfo;
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();

  private KeyProviderCryptoExtension kmsProvider = null;
  private static String keyProviderUriKeyName =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;

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

  private boolean isNativeAuthorizerEnabled;

  private ExitManager exitManager;

  private enum State {
    INITIALIZED,
    RUNNING,
    STOPPED
  }

  // Used in MiniOzoneCluster testing
  private State omState;

  private Thread emptier;

  @SuppressWarnings("methodlength")
  private OzoneManager(OzoneConfiguration conf) throws IOException,
      AuthenticationException {
    super(OzoneVersionInfo.OZONE_VERSION_INFO);
    Preconditions.checkNotNull(conf);
    configuration = conf;
    // Load HA related configurations
    OMHANodeDetails omhaNodeDetails =
        OMHANodeDetails.loadOMHAConfig(configuration);

    this.peerNodes = omhaNodeDetails.getPeerNodeDetails();
    this.omNodeDetails = omhaNodeDetails.getLocalNodeDetails();

    omStorage = new OMStorage(conf);
    omId = omStorage.getOmId();

    loginOMUserIfSecurityEnabled(conf);

    this.allowListAllVolumes = conf.getBoolean(OZONE_OM_VOLUME_LISTALL_ALLOWED,
        OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT);
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
    this.isAclEnabled = conf.getBoolean(OZONE_ACL_ENABLED,
        OZONE_ACL_ENABLED_DEFAULT);
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

    InetSocketAddress omNodeRpcAddr = omNodeDetails.getRpcAddress();
    omRpcAddressTxt = new Text(omNodeDetails.getRpcAddressString());

    scmContainerClient = getScmContainerClient(configuration);
    // verifies that the SCM info in the OM Version file is correct.
    scmBlockClient = getScmBlockClient(configuration);
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);

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

    instantiateServices();

    // Create special volume s3v which is required for S3G.
    addS3GVolumeToDB();

    this.omRatisSnapshotInfo = new RatisSnapshotInfo();

    if (isRatisEnabled) {
      // Create Ratis storage dir
      String omRatisDirectory =
          OzoneManagerRatisServer.getOMRatisDirectory(configuration);
      if (omRatisDirectory == null || omRatisDirectory.isEmpty()) {
        throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
            " must be defined.");
      }
      OmUtils.createOMDir(omRatisDirectory);

      // Create Ratis snapshot dir
      omRatisSnapshotDir = OmUtils.createOMDir(
          OzoneManagerRatisServer.getOMRatisSnapshotDirectory(configuration));

      // Before starting ratis server, check if previous installation has
      // snapshot directory in Ratis storage directory. if yes, move it to
      // new snapshot directory.

      File snapshotDir = new File(omRatisDirectory, OM_RATIS_SNAPSHOT_DIR);

      if (snapshotDir.isDirectory()) {
        FileUtils.moveDirectory(snapshotDir.toPath(),
            omRatisSnapshotDir.toPath());
      }

      if (peerNodes != null && !peerNodes.isEmpty()) {
        this.omSnapshotProvider = new OzoneManagerSnapshotProvider(
            configuration, omRatisSnapshotDir, peerNodes);
      }
    }

    initializeRatisServer();

    metrics = OMMetrics.create();
    omClientProtocolMetrics = ProtocolMessageMetrics
        .create("OmClientProtocol", "Ozone Manager RPC endpoint",
            OzoneManagerProtocolProtos.Type.values());

    // Start Om Rpc Server.
    omRpcServer = getRpcServer(configuration);
    omRpcAddress = updateRPCListenAddress(configuration,
        OZONE_OM_ADDRESS_KEY, omNodeRpcAddr, omRpcServer);

    shutdownHook = () -> {
      saveOmMetrics();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
    omState = State.INITIALIZED;
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
  private void instantiateServices() throws IOException {

    metadataManager = new OmMetadataManagerImpl(configuration);
    volumeManager = new VolumeManagerImpl(metadataManager, configuration);
    bucketManager = new BucketManagerImpl(metadataManager, getKmsProvider(),
        isRatisEnabled);
    if (secConfig.isSecurityEnabled()) {
      s3SecretManager = new S3SecretManagerImpl(configuration, metadataManager);
      delegationTokenMgr = createDelegationTokenSecretManager(configuration);
    }

    prefixManager = new PrefixManagerImpl(metadataManager, isRatisEnabled);
    keyManager = new KeyManagerImpl(this, scmClient, configuration,
        omStorage.getOmId());

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
        authorizer.setOzoneAdmins(getOzoneAdmins(configuration));
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
      Files.createDirectories(
          getTempMetricsStorageFile().getParentFile().toPath());
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
      readKeyPair();
    } catch (OzoneSecurityException e) {
      LOG.error("Unable to read key pair for OM.", e);
      throw new RuntimeException(e);
    }
    if (secConfig.isBlockTokenEnabled() && blockTokenMgr != null) {
      try {
        LOG.info("Starting OM block token secret manager");
        blockTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting block token secret manager.", e);
        throw new RuntimeException(e);
      }
    }

    if (delegationTokenMgr != null) {
      try {
        LOG.info("Starting OM delegation token secret manager");
        delegationTokenMgr.start(certClient);
      } catch (IOException e) {
        // Unable to start secret manager.
        LOG.error("Error starting delegation token secret manager.", e);
        throw new RuntimeException(e);
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
   * Read private key from file.
   */
  private void readKeyPair() throws OzoneSecurityException {
    try {
      LOG.info("Reading keypair and certificate from file system.");
      PublicKey pubKey = certClient.getPublicKey();
      PrivateKey pvtKey = certClient.getPrivateKey();
      Objects.requireNonNull(pubKey);
      Objects.requireNonNull(pvtKey);
      Objects.requireNonNull(certClient.getCertificate());
    } catch (Exception e) {
      throw new OzoneSecurityException("Error reading keypair & certificate "
          + "OzoneManager.", e, OzoneSecurityException
          .ResultCodes.OM_PUBLIC_PRIVATE_KEY_FILE_NOT_EXIST);
    }
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
   * Starts an RPC server, if configured.
   *
   * @param conf         configuration
   * @param addr         configured address of RPC server
   * @param protocol     RPC protocol provided by RPC server
   * @param instance     RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount) throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(delegationTokenMgr)
        .build();

    HddsServerUtil.addPBProtocol(conf, protocol, instance, rpcServer);

    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      rpcServer.refreshServiceAcl(conf, OMPolicyProvider.getInstance());
    }
    return rpcServer;
  }

  private static boolean isOzoneSecurityEnabled() {
    return securityEnabled;
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
    return new OzoneManager(conf);
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

  /**
   * Get metadata manager.
   *
   * @return metadata manager.
   */
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
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
    omClientProtocolMetrics.register();
    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    // Start Ratis services
    if (omRatisServer != null) {
      omRatisServer.start();
    }

    metadataManager.start(configuration);
    startSecretManagerIfNecessary();


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

    registerMXBean();

    startJVMPauseMonitor();
    setStartTime();
    omState = State.RUNNING;

  }

  /**
   * Restarts the service. This method re-initializes the rpc server.
   */
  public void restart() throws IOException {
    LOG.info(buildRpcServerStartMessage("OzoneManager RPC server",
        omRpcAddress));

    HddsServerUtil.initializeMetrics(configuration, "OzoneManager");

    instantiateServices();

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

    // Schedule save metrics
    long period = configuration.getTimeDuration(OZONE_OM_METRICS_SAVE_INTERVAL,
        OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    scheduleOMMetricsWriteTask = new ScheduleOMMetricsWriteTask();
    metricsTimer = new Timer();
    metricsTimer.schedule(scheduleOMMetricsWriteTask, 0, period);

    initializeRatisServer();
    if (omRatisServer != null) {
      omRatisServer.start();
    }

    omRpcServer = getRpcServer(configuration);

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

    startJVMPauseMonitor();
    setStartTime();
    omState = State.RUNNING;
  }


  /**
   * Starts a Trash Emptier thread that does an fs.trashRoots and performs
   * checkpointing & deletion.
   * @param conf
   * @throws IOException
   */
  private void startTrashEmptier(Configuration conf) throws IOException {
    long hadoopTrashInterval =
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    // check whether user has configured ozone specific trash-interval
    // if not fall back to hadoop configuration
    long trashInterval =
            conf.getLong(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY,
                hadoopTrashInterval);
    if (trashInterval == 0) {
      LOG.info("Trash Interval set to 0. Files deleted will not move to trash");
      return;
    } else if (trashInterval < 0) {
      throw new IOException("Cannot start trash emptier with negative interval."
              + " Set " + FS_TRASH_INTERVAL_KEY + " to a positive value.");
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

  /**
   * Creates a new instance of rpc server. If an earlier instance is already
   * running then returns the same.
   */
  private RPC.Server getRpcServer(OzoneConfiguration conf) throws IOException {
    if (isOmRpcServerRunning) {
      return omRpcServer;
    }

    InetSocketAddress omNodeRpcAddr = OmUtils.getOmAddress(conf);

    final int handlerCount = conf.getInt(OZONE_OM_HANDLER_COUNT_KEY,
        OZONE_OM_HANDLER_COUNT_DEFAULT);
    RPC.setProtocolEngine(configuration, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    this.omServerProtocol = new OzoneManagerProtocolServerSideTranslatorPB(
        this, omRatisServer, omClientProtocolMetrics, isRatisEnabled,
        getLastTrxnIndexForNonRatis());

    BlockingService omService = newReflectiveBlockingService(omServerProtocol);

    return startRpcServer(configuration, omNodeRpcAddr,
        OzoneManagerProtocolPB.class, omService,
        handlerCount);
  }

  /**
   * Creates an instance of ratis server.
   */
  private void initializeRatisServer() throws IOException {
    if (isRatisEnabled) {
      if (omRatisServer == null) {
        // This needs to be done before initializing Ratis.
        RatisDropwizardExports.
            registerRatisMetricReporters(ratisMetricsMap);
        omRatisServer = OzoneManagerRatisServer.newOMRatisServer(
            configuration, this, omNodeDetails, peerNodes,
            secConfig, certClient);
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

  public RatisSnapshotInfo getSnapshotInfo() {
    return omRatisSnapshotInfo;
  }

  @VisibleForTesting
  public long getRatisSnapshotIndex() throws IOException {
    return TransactionInfo.readTransactionInfo(metadataManager)
        .getTransactionIndex();
  }

  /**
   * Stop service.
   */
  public void stop() {
    try {
      // Cancel the metrics timer and set to null.
      if (metricsTimer != null) {
        metricsTimer.cancel();
        metricsTimer = null;
        scheduleOMMetricsWriteTask = null;
      }
      omRpcServer.stop();
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
      keyManager.stop();
      stopSecretManager();
      if (httpServer != null) {
        httpServer.stop();
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
      omState = State.STOPPED;
    } catch (Exception e) {
      LOG.error("OzoneManager stop failed.", e);
    }
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
    InetSocketAddress omRpcAdd;
    omRpcAdd = OmUtils.getOmAddress(config);
    if (omRpcAdd == null || omRpcAdd.getAddress() == null) {
      LOG.error("Incorrect om rpc address. omRpcAdd:{}", omRpcAdd);
      throw new RuntimeException("Can't get SCM signed certificate. " +
          "omRpcAdd: " + omRpcAdd);
    }
    // Get host name.
    String hostname = omRpcAdd.getAddress().getHostName();
    String ip = omRpcAdd.getAddress().getHostAddress();

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
            .setHostName(omRpcAdd.getHostName())
            .setIpAddress(ip)
            .setUuid(omStore.getOmId())
            .addPorts(HddsProtos.Port.newBuilder()
                .setName(RPC_PORT)
                .setValue(omRpcAdd.getPort())
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
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(OmVolumeArgs args) throws IOException {
    try {
      metrics.incNumVolumeCreates();
      if (isAclEnabled) {
        checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.CREATE,
            args.getVolume(), null, null);
      }
      volumeManager.createVolume(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.CREATE_VOLUME,
          (args == null) ? null : args.toAuditMap()));
      metrics.incNumVolumes();
    } catch (Exception ex) {
      metrics.incNumVolumeCreateFails();
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(OMAction.CREATE_VOLUME,
              (args == null) ? null : args.toAuditMap(), ex)
      );
      throw ex;
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
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    checkAcls(resType, store, acl, vol, bucket, key,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
        remoteIp != null ? remoteIp.getHostName() : omRpcAddress.getHostName(),
        true, getVolumeOwner(vol, acl, resType));
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
      boolean throwIfPermissionDenied, String volumeOwner)
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
        .setOwnerName(volumeOwner)
        .build();
    if (!accessAuthorizer.checkAccess(obj, context)) {
      if (throwIfPermissionDenied) {
        LOG.warn("User {} doesn't have {} permission to access {} /{}/{}/{}",
            context.getClientUgi().getUserName(), context.getAclRights(),
            obj.getResourceType(), obj.getVolumeName(), obj.getBucketName(),
            obj.getKeyName());
        throw new OMException("User " + context.getClientUgi().getUserName() +
            " doesn't have " + context.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            obj.getVolumeName() + " " + obj.getBucketName() + " " +
            obj.getKeyName(),
            ResultCodes.PERMISSION_DENIED);
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
   * {@inheritDoc}
   */
  @Override
  public boolean setOwner(String volume, String owner) throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.WRITE_ACL, volume,
          null, null);
    }
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.OWNER, owner);
    try {
      metrics.incNumVolumeUpdates();
      volumeManager.setOwner(volume, owner);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.SET_OWNER,
          auditMap));
      return true;
    } catch (Exception ex) {
      metrics.incNumVolumeUpdateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.SET_OWNER,
          auditMap, ex)
      );
      throw ex;
    }
  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quotaInNamespace - Volume quota in counts.
   * @param quotaInBytes - Volume quota in bytes.
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quotaInNamespace,
                       long quotaInBytes) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented. As this requests use a new approach");
  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume  - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume, false
   * otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.VOLUME, StoreType.OZONE,
          ACLType.READ, volume, null, null);
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.USER_ACL,
        (userAcl == null) ? null : userAcl.getName());
    try {
      metrics.incNumVolumeCheckAccesses();
      return volumeManager.checkVolumeAccess(volume, userAcl);
    } catch (Exception ex) {
      metrics.incNumVolumeCheckAccessFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          OMAction.CHECK_VOLUME_ACCESS, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.CHECK_VOLUME_ACCESS, auditMap));
      }
    }
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
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {
    try {
      if (isAclEnabled) {
        checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.DELETE, volume,
            null, null);
      }
      metrics.incNumVolumeDeletes();
      volumeManager.deleteVolume(volume);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_VOLUME,
          buildAuditMap(volume)));
      metrics.decNumVolumes();
    } catch (Exception ex) {
      metrics.incNumVolumeDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_VOLUME,
          buildAuditMap(volume), ex));
      throw ex;
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
   * Creates a bucket.
   *
   * @param bucketInfo - BucketInfo to create bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    try {
      if (isAclEnabled) {
        checkAcls(ResourceType.VOLUME, StoreType.OZONE, ACLType.CREATE,
            bucketInfo.getVolumeName(), bucketInfo.getBucketName(), null);
      }
      metrics.incNumBucketCreates();
      bucketManager.createBucket(bucketInfo);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.CREATE_BUCKET,
          (bucketInfo == null) ? null : bucketInfo.toAuditMap()));
      metrics.incNumBuckets();
    } catch (Exception ex) {
      metrics.incNumBucketCreateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.CREATE_BUCKET,
          (bucketInfo == null) ? null : bucketInfo.toAuditMap(), ex));
      throw ex;
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
      return bucketManager.getBucketInfo(volume, bucket);
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
   * Allocate a key.
   *
   * @param args - attributes of the key.
   * @return OpenKeySession - a handler to key that client uses to
   * talk to container.
   * @throws IOException
   */
  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            bucket.realVolume(), bucket.realBucket(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              bucket.realVolume(), bucket.realBucket(), args.getKeyName());
        } else {
          throw ex;
        }
      }
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumKeyAllocates();
      return keyManager.openKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyAllocateFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.ALLOCATE_KEY,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.ALLOCATE_KEY, auditMap));
      }
    }
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID)
      throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            bucket.realVolume(), bucket.realBucket(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              bucket.realVolume(), bucket.realBucket(), args.getKeyName());
        } else {
          throw ex;
        }
      }
    }

    Map<String, String> auditMap = bucket.audit(args.toAuditMap());
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    args = bucket.update(args);

    try {
      metrics.incNumKeyCommits();
      keyManager.commitKey(args, clientID);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.COMMIT_KEY,
          auditMap));
      // As when we commit the key it is visible, so we should increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.
      if (args.getLocationInfoList() != null &&
          args.getLocationInfoList().size() > 0 &&
          args.getLocationInfoList().get(0) != null &&
          args.getLocationInfoList().get(0).getCreateVersion() == 0) {
        metrics.incNumKeys();
      }
    } catch (Exception ex) {
      metrics.incNumKeyCommitFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.COMMIT_KEY,
          auditMap, ex));
      throw ex;
    }
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      try {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
            bucket.realVolume(), bucket.realBucket(), args.getKeyName());
      } catch (OMException ex) {
        // For new keys key checkAccess call will fail as key doesn't exist.
        // Check user access for bucket.
        if (ex.getResult().equals(KEY_NOT_FOUND)) {
          checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
              bucket.realVolume(), bucket.realBucket(), args.getKeyName());
        } else {
          throw ex;
        }
      }
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    args = bucket.update(args);

    try {
      metrics.incNumBlockAllocateCalls();
      return keyManager.allocateBlock(args, clientID, excludeList);
    } catch (Exception ex) {
      metrics.incNumBlockAllocateCallFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.ALLOCATE_BLOCK,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.ALLOCATE_BLOCK, auditMap));
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
  public void renameKeys(OmRenameKeys omRenameKeys)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented. As write requests use a new approach");
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    Preconditions.checkNotNull(args);

    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    Map<String, String> auditMap = bucket.audit(args.toAuditMap());
    auditMap.put(OzoneConsts.TO_KEY_NAME, toKeyName);

    args = bucket.update(args);

    try {
      metrics.incNumKeyRenames();
      keyManager.renameKey(args, toKeyName);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.RENAME_KEY,
          auditMap));
    } catch (IOException e) {
      metrics.incNumKeyRenameFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.RENAME_KEY,
          auditMap, e));
      throw e;
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param args - attributes of the key.
   * @throws IOException
   */
  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    Map<String, String> auditMap = args.toAuditMap();
    try {
      ResolvedBucket bucket = resolveBucketLink(args);
      args = bucket.update(args);

      if (isAclEnabled) {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.DELETE,
            bucket.realVolume(), bucket.realBucket(), args.getKeyName());
      }

      metrics.incNumKeyDeletes();
      keyManager.deleteKey(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_KEY,
          auditMap));
      metrics.decNumKeys();
    } catch (Exception ex) {
      metrics.incNumKeyDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_KEY,
          auditMap, ex));
      throw ex;
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param deleteKeys - List of keys to be deleted from volume and a bucket.
   * @throws IOException
   */
  @Override
  public void deleteKeys(OmDeleteKeys deleteKeys) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented. As write requests use a new approach");
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

  /**
   * Sets bucket property from args.
   *
   * @param args - BucketArgs.
   * @throws IOException
   */
  @Override
  public void setBucketProperty(OmBucketArgs args)
      throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE,
          args.getVolumeName(), args.getBucketName(), null);
    }
    try {
      metrics.incNumBucketUpdates();
      bucketManager.setBucketProperty(args);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.UPDATE_BUCKET,
          (args == null) ? null : args.toAuditMap()));
    } catch (Exception ex) {
      metrics.incNumBucketUpdateFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.UPDATE_BUCKET,
          (args == null) ? null : args.toAuditMap(), ex));
      throw ex;
    }
  }

  /**
   * Deletes an existing empty bucket from volume.
   *
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  @Override
  public void deleteBucket(String volume, String bucket) throws IOException {
    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.WRITE, volume,
          bucket, null);
    }
    Map<String, String> auditMap = buildAuditMap(volume);
    auditMap.put(OzoneConsts.BUCKET, bucket);
    try {
      metrics.incNumBucketDeletes();
      bucketManager.deleteBucket(volume, bucket);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction.DELETE_BUCKET,
          auditMap));
      metrics.decNumBuckets();
    } catch (Exception ex) {
      metrics.incNumBucketDeleteFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.DELETE_BUCKET,
          auditMap, ex));
      throw ex;
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

      for (OMNodeDetails peerNode : peerNodes) {
        ServiceInfo.Builder peerOmServiceInfoBuilder = ServiceInfo.newBuilder()
            .setNodeType(HddsProtos.NodeType.OM)
            .setHostname(peerNode.getHostName())
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
    // not block protocol.
    InetSocketAddress scmAddr = getScmAddressForClients(
        configuration);
    ServiceInfo.Builder scmServiceInfoBuilder = ServiceInfo.newBuilder()
        .setNodeType(HddsProtos.NodeType.SCM)
        .setHostname(scmAddr.getHostName())
        .addServicePort(ServicePort.newBuilder()
            .setType(ServicePort.Type.RPC)
            .setValue(scmAddr.getPort()).build());
    services.add(scmServiceInfoBuilder.build());

    metrics.incNumGetServiceLists();
    // For now there is no exception that can can happen in this call,
    // so failure metrics is not handled. In future if there is any need to
    // handle exception in this method, we need to incorporate
    // metrics.incNumGetServiceListFails()
    return services;
  }

  @Override
  public ServiceInfoEx getServiceInfo() throws IOException {
    return new ServiceInfoEx(getServiceList(), caCertPem, caCertPemList);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();

    // Check whether user name passed is matching with the current user or not.
    if (!user.getUserName().equals(kerberosID)) {
      throw new OMException("User mismatch. Requested user name is " +
          "mismatched " + kerberosID + ", with current user " +
          user.getUserName(), OMException.ResultCodes.USER_MISMATCH);
    }
    return s3SecretManager.getS3Secret(kerberosID);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs keyArgs) throws
      IOException {

    Preconditions.checkNotNull(keyArgs);
    ResolvedBucket bucket = resolveBucketLink(keyArgs);

    Map<String, String> auditMap = bucket.audit(keyArgs.toAuditMap());

    keyArgs = bucket.update(keyArgs);

    metrics.incNumInitiateMultipartUploads();
    try {
      OmMultipartInfo result = keyManager.initiateMultipartUpload(keyArgs);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          OMAction.INITIATE_MULTIPART_UPLOAD, auditMap));
      return result;
    } catch (IOException ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          OMAction.INITIATE_MULTIPART_UPLOAD, auditMap, ex));
      metrics.incNumInitiateMultipartUploadFails();
      throw ex;
    }
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs keyArgs, long clientID) throws IOException {

    Preconditions.checkNotNull(keyArgs);
    ResolvedBucket bucket = resolveBucketLink(keyArgs);

    Map<String, String> auditMap = bucket.audit(keyArgs.toAuditMap());

    keyArgs = bucket.update(keyArgs);

    metrics.incNumCommitMultipartUploadParts();
    try {
      OmMultipartCommitUploadPartInfo result =
          keyManager.commitMultipartUploadPart(keyArgs, clientID);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          OMAction.COMMIT_MULTIPART_UPLOAD_PARTKEY, auditMap));
      return result;
    } catch (IOException ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          OMAction.INITIATE_MULTIPART_UPLOAD, auditMap, ex));
      metrics.incNumCommitMultipartUploadPartFails();
      throw ex;
    }
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadCompleteList multipartUploadList)
      throws IOException {

    Preconditions.checkNotNull(omKeyArgs);
    ResolvedBucket bucket = resolveBucketLink(omKeyArgs);

    Map<String, String> auditMap = bucket.audit(omKeyArgs.toAuditMap());
    auditMap.put(OzoneConsts.MULTIPART_LIST, multipartUploadList
        .getMultipartMap().toString());

    omKeyArgs = bucket.update(omKeyArgs);

    metrics.incNumCompleteMultipartUploads();
    try {
      OmMultipartUploadCompleteInfo result = keyManager.completeMultipartUpload(
              omKeyArgs, multipartUploadList);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap));
      return result;
    } catch (IOException ex) {
      metrics.incNumCompleteMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {

    Preconditions.checkNotNull(omKeyArgs);
    ResolvedBucket bucket = resolveBucketLink(omKeyArgs);

    Map<String, String> auditMap = bucket.audit(omKeyArgs.toAuditMap());

    omKeyArgs = bucket.update(omKeyArgs);

    metrics.incNumAbortMultipartUploads();
    try {
      keyManager.abortMultipartUpload(omKeyArgs);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap));
    } catch (IOException ex) {
      metrics.incNumAbortMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
          .COMPLETE_MULTIPART_UPLOAD, auditMap, ex));
      throw ex;
    }

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
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .LIST_MULTIPART_UPLOAD_PARTS, auditMap));
      return omMultipartUploadListParts;
    } catch (IOException ex) {
      metrics.incNumListMultipartUploadPartFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
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
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(OMAction
          .LIST_MULTIPART_UPLOADS, auditMap));
      return omMultipartUploadList;

    } catch (IOException ex) {
      metrics.incNumListMultipartUploadFails();
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction
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
  public void createDirectory(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumCreateDirectory();
      keyManager.createDirectory(args);
    } catch (IOException ex) {
      metrics.incNumCreateDirectoryFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(OMAction.CREATE_DIRECTORY, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(OMAction.CREATE_DIRECTORY, auditMap));
      }
    }
  }

  @Override
  public OpenKeySession createFile(OmKeyArgs args, boolean overWrite,
      boolean recursive) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumCreateFile();
      return keyManager.createFile(args, overWrite, recursive);
    } catch (Exception ex) {
      metrics.incNumCreateFileFails();
      auditSuccess = false;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.CREATE_FILE,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.CREATE_FILE, auditMap));
      }
    }
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
      AUDIT.logWriteFailure(buildAuditMessageForFailure(OMAction.LOOKUP_FILE,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            OMAction.LOOKUP_FILE, auditMap));
      }
    }
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries) throws IOException {

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
              getClientAddress());
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

  private void auditAcl(OzoneObj ozoneObj, List<OzoneAcl> ozoneAcl,
      OMAction omAction, Exception ex) {
    Map<String, String> auditMap = ozoneObj.toAuditMap();
    if (ozoneAcl != null) {
      auditMap.put(OzoneConsts.ACL, ozoneAcl.toString());
    }

    if (ex == null) {
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(omAction, auditMap));
    } else {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(omAction, auditMap, ex));
    }
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
            obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
      }
      metrics.incNumAddAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.addAcl(obj, acl);
      case BUCKET:
        return bucketManager.addAcl(obj, acl);
      case KEY:
        return keyManager.addAcl(obj, acl);
      case PREFIX:
        return prefixManager.addAcl(obj, acl);
      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      auditAcl(obj, Arrays.asList(acl), OMAction.ADD_ACL, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditAcl(obj, Arrays.asList(acl), OMAction.ADD_ACL, null);
      }
    }
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
            obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
      }
      metrics.incNumRemoveAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.removeAcl(obj, acl);
      case BUCKET:
        return bucketManager.removeAcl(obj, acl);
      case KEY:
        return keyManager.removeAcl(obj, acl);
      case PREFIX:
        return prefixManager.removeAcl(obj, acl);

      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      auditAcl(obj, Arrays.asList(acl), OMAction.REMOVE_ACL, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditAcl(obj, Arrays.asList(acl), OMAction.REMOVE_ACL, null);
      }
    }
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj  Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.WRITE_ACL,
            obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
      }
      metrics.incNumSetAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.setAcl(obj, acls);
      case BUCKET:
        return bucketManager.setAcl(obj, acls);
      case KEY:
        return keyManager.setAcl(obj, acls);
      case PREFIX:
        return prefixManager.setAcl(obj, acls);
      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      auditAcl(obj, acls, OMAction.SET_ACL, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditAcl(obj, acls, OMAction.SET_ACL, null);
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
      auditAcl(obj, null, OMAction.GET_ACL, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditAcl(obj, null, OMAction.GET_ACL, null);
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

    File oldDBLocation = metadataManager.getStore().getDbLocation();
    try {
      // Stop Background services
      stopServices();

      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      omRatisServer.getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // During stopServices, if KeyManager was stopped successfully and
      // OMMetadataManager stop failed, we should restart the KeyManager.
      keyManager.start(configuration);
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

    if (canProceed) {
      try {
        dbBackup = replaceOMDBWithCheckpoint(lastAppliedIndex, oldDBLocation,
            checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, index: {}",
            leaderId, term, lastAppliedIndex);
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
      reloadOMState(lastAppliedIndex, term);
      omRatisServer.getOmStateMachine().unpause(lastAppliedIndex, term);
      LOG.info("Reloaded OM state with Term: {} and Index: {}", term,
          lastAppliedIndex);
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      exitManager.exitSystem(1, errorMsg, ex, LOG);
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

  void stopServices() throws Exception {
    keyManager.stop();
    stopSecretManager();
    metadataManager.stop();
    stopTrashEmptier();
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
      Files.move(checkpointPath, oldDB.toPath());
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
        ExitUtils.terminate(1, errorMsg, ex, LOG);
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

    instantiateServices();

    // Restart required services
    metadataManager.start(configuration);
    keyManager.start(configuration);
    startTrashEmptier(configuration);

    // Set metrics and start metrics back ground thread
    metrics.setNumVolumes(metadataManager.countRowsInTable(metadataManager
        .getVolumeTable()));
    metrics.setNumBuckets(metadataManager.countRowsInTable(metadataManager
        .getBucketTable()));
    metrics.setNumKeys(metadataManager.countEstimatedRowsInTable(metadataManager
        .getKeyTable()));

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

  public static void setTestSecureOmFlag(boolean testSecureOmFlag) {
    OzoneManager.testSecureOmFlag = testSecureOmFlag;
  }

  public String getOMNodeId() {
    return omNodeDetails.getNodeId();
  }

  public String getOMServiceId() {
    return omNodeDetails.getServiceId();
  }

  @VisibleForTesting
  public List<OMNodeDetails> getPeerNodes() {
    return peerNodes;
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
    DBUpdatesWrapper updatesSince = metadataManager.getStore()
        .getUpdatesSince(dbUpdatesRequest.getSequenceNumber());
    DBUpdates dbUpdates = new DBUpdates(updatesSince.getData());
    dbUpdates.setCurrentSequenceNumber(updatesSince.getCurrentSequenceNumber());
    return dbUpdates;
  }

  public OzoneDelegationTokenSecretManager getDelegationTokenMgr() {
    return delegationTokenMgr;
  }

  /**
   * Return list of OzoneAdministrators.
   */
  Collection<String> getOzoneAdmins(OzoneConfiguration conf)
      throws IOException {
    Collection<String> ozAdmins =
        conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!ozAdmins.contains(omSPN)) {
      ozAdmins.add(omSPN);
    }
    return ozAdmins;
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
    try {
      if (isAclEnabled) {
        InetAddress remoteIp = Server.getRemoteIp();
        resolved = resolveBucketLink(requested, new HashSet<>(),
            Server.getRemoteUser(),
            remoteIp,
            remoteIp != null ? remoteIp.getHostName() :
                omRpcAddress.getHostName());
      } else {
        resolved = resolveBucketLink(requested, new HashSet<>(),
            null, null, null);
      }
    } catch (Throwable t) {
      throw t;
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
  void setExitManagerForTesting(ExitManager exitManagerForTesting) {
    this.exitManager = exitManagerForTesting;
  }


  public boolean getEnableFileSystemPaths() {
    return configuration.getBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);
  }

  /**
   * Create volume which is required for S3Gateway operations.
   * @throws IOException
   */
  private void addS3GVolumeToDB() throws IOException {
    String s3VolumeName = HddsClientUtils.getS3VolumeName(configuration);
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

      OmVolumeArgs omVolumeArgs = createS3VolumeInfo(s3VolumeName, objectID);

      String dbUserKey = metadataManager.getUserKey(userName);
      PersistedUserVolumeInfo userVolumeInfo =
          PersistedUserVolumeInfo.newBuilder()
          .setObjectID(objectID)
          .setUpdateID(transactionID)
          .addVolumeNames(s3VolumeName).build();


      // Commit to DB.
      try(BatchOperation batchOperation =
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

  private OmVolumeArgs createS3VolumeInfo(String s3Volume,
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

  public int getMinMultipartUploadPartSize() {
    return minMultipartUploadPartSize;
  }

  @VisibleForTesting
  public void setMinMultipartUploadPartSize(int partSizeForTest) {
    this.minMultipartUploadPartSize = partSizeForTest;
  }

}
