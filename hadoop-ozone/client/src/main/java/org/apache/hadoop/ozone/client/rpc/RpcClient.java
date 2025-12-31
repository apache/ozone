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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.ozone.OzoneAcl.LINK_BUCKET_DEFAULT_ACL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_ELASTIC_BYTE_BUFFER_POOL_MAX_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_ELASTIC_BYTE_BUFFER_POOL_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD;
import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_MAXIMUM_ACCESS_ID_LENGTH;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneMultipartUpload;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.TenantArgs;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.BoundedElasticByteBufferPool;
import org.apache.hadoop.ozone.client.io.CipherOutputStreamOzone;
import org.apache.hadoop.ozone.client.io.ECBlockInputStream;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.io.OzoneCryptoInputStream;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmTenantArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.GDPRSymmetricKey;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone RPC Client Implementation, it connects to OM, SCM and DataNode
 * to execute client calls. This uses RPC protocol for communication
 * with the servers.
 */
public class RpcClient implements ClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RpcClient.class);

  // For the minimal recommended EC policy rs-3-2-1024k,
  // we should have at least 1 core thread for each necessary chunk
  // for reconstruction.
  private static final int EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE = 3;

  private static final int WRITE_POOL_MIN_SIZE = 1;

  private final ConfigurationSource conf;
  private final OzoneManagerClientProtocol ozoneManagerClient;
  private final XceiverClientFactory xceiverClientManager;
  private final UserGroupInformation ugi;
  private UserGroupInformation s3gUgi;
  private final ClientId clientId = ClientId.randomId();
  private final boolean unsafeByteBufferConversion;
  private Text dtService;
  private final boolean topologyAwareReadEnabled;
  private final boolean checkKeyNameEnabled;
  private final OzoneClientConfig clientConfig;
  private final ReplicationConfigValidator replicationConfigValidator;
  private final Cache<URI, KeyProvider> keyProviderCache;
  private final boolean getLatestVersionLocation;
  private final ByteBufferPool byteBufferPool;
  private final BlockInputStreamFactory blockInputStreamFactory;
  private final OzoneManagerVersion omVersion;
  private final MemoizedSupplier<ExecutorService> ecReconstructExecutor;
  private final ContainerClientMetrics clientMetrics;
  private final MemoizedSupplier<ExecutorService> writeExecutor;
  private final AtomicBoolean isS3GRequest = new AtomicBoolean(false);
  private volatile OzoneFsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  private final long serverDefaultsValidityPeriod;

  /**
   * Creates RpcClient instance with the given configuration.
   *
   * @param conf        Configuration
   * @param omServiceId OM HA Service ID, set this to null if not HA
   * @throws IOException
   */
  public RpcClient(ConfigurationSource conf, String omServiceId)
      throws IOException {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    replicationConfigValidator =
        this.conf.getObject(ReplicationConfigValidator.class);

    this.clientConfig = conf.getObject(OzoneClientConfig.class);
    this.ecReconstructExecutor = MemoizedSupplier.valueOf(() -> createThreadPoolExecutor(
        EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE, clientConfig.getEcReconstructStripeReadPoolLimit(),
        "ec-reconstruct-reader-TID-%d"));
    this.writeExecutor = MemoizedSupplier.valueOf(() -> createThreadPoolExecutor(
        WRITE_POOL_MIN_SIZE, Integer.MAX_VALUE, "client-write-TID-%d"));

    OmTransport omTransport = createOmTransport(omServiceId);
    OzoneManagerProtocolClientSideTranslatorPB
        ozoneManagerProtocolClientSideTranslatorPB =
        new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
        clientId.toString());
    this.ozoneManagerClient = TracingUtil.createProxy(
        ozoneManagerProtocolClientSideTranslatorPB,
        OzoneManagerClientProtocol.class, conf);
    if (getThreadLocalS3Auth() != null) {
      this.s3gUgi = UserGroupInformation.createRemoteUser(getThreadLocalS3Auth().getUserPrincipal());
    }
    dtService = omTransport.getDelegationTokenService();
    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    omVersion = getOmVersion(serviceInfoEx);
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      // If the client is authenticating using S3 style auth, all future
      // requests serviced by this client will need S3 Auth set.
      boolean isS3 = conf.getBoolean(S3Auth.S3_AUTH_CHECK, false);
      ozoneManagerProtocolClientSideTranslatorPB.setS3AuthCheck(isS3);
      if (isS3) {
        // S3 Auth works differently and needs OM version to be at 2.0.0
        OzoneManagerVersion minOmVersion = conf.getEnum(
            OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
            OzoneManagerVersion.DEFAULT_VERSION);
        if (!validateOmVersion(
            minOmVersion, serviceInfoEx.getServiceInfoList())) {
          if (LOG.isDebugEnabled()) {
            for (ServiceInfo s : serviceInfoEx.getServiceInfoList()) {
              LOG.debug("Node {} version {}", s.getHostname(),
                  s.getProtobuf().getOMVersion());
            }
          }
          throw new RuntimeException(
              "Minimum OzoneManager version required is: " + minOmVersion
              + ", in the service list there are not enough Ozone Managers"
              + " meet the criteria.");
        }
      }
    }

    this.xceiverClientManager = createXceiverClientFactory(serviceInfoEx);

    unsafeByteBufferConversion = conf.getBoolean(
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);

    topologyAwareReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    checkKeyNameEnabled = conf.getObject(OmConfig.class)
        .isKeyNameCharacterCheckEnabled();
    getLatestVersionLocation = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION,
        OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION_DEFAULT);

    long keyProviderCacheExpiryMs = conf.getTimeDuration(
        OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY,
        OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT, TimeUnit.MILLISECONDS);
    keyProviderCache = CacheBuilder.newBuilder()
        .expireAfterAccess(keyProviderCacheExpiryMs, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<URI, KeyProvider>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<URI, KeyProvider> notification) {
            try {
              assert notification.getValue() != null;
              notification.getValue().close();
            } catch (Throwable t) {
              LOG.error("Error closing KeyProvider with uri [" +
                  notification.getKey() + "]", t);
            }
          }
        }).build();
    long maxPoolSize = (long) conf.getStorageSize(
        OZONE_CLIENT_ELASTIC_BYTE_BUFFER_POOL_MAX_SIZE,
        OZONE_CLIENT_ELASTIC_BYTE_BUFFER_POOL_MAX_SIZE_DEFAULT,
        StorageUnit.GB);
    this.byteBufferPool = new BoundedElasticByteBufferPool(maxPoolSize);
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, ecReconstructExecutor);
    this.clientMetrics = ContainerClientMetrics.acquire();

    this.serverDefaultsValidityPeriod = conf.getTimeDuration(
        OZONE_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS,
        OZONE_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_DEFAULT,
        TimeUnit.MILLISECONDS);

    TracingUtil.initTracing("client", conf);
  }

  public XceiverClientFactory getXceiverClientManager() {
    return xceiverClientManager;
  }

  public static OzoneManagerVersion getOmVersion(ServiceInfoEx info) {
    OzoneManagerVersion version = OzoneManagerVersion.CURRENT;
    for (ServiceInfo si : info.getServiceInfoList()) {
      if (si.getNodeType() == HddsProtos.NodeType.OM) {
        OzoneManagerVersion current =
            OzoneManagerVersion.fromProtoValue(si.getProtobuf().getOMVersion());
        if (version.compareTo(current) > 0) {
          version = current;
        }
      }
    }
    LOG.trace("Ozone Manager version is {}", version.name());
    return version;
  }

  static boolean validateOmVersion(OzoneManagerVersion minimumVersion,
                                   List<ServiceInfo> serviceInfoList) {
    if (minimumVersion == OzoneManagerVersion.FUTURE_VERSION) {
      // A FUTURE_VERSION should not be expected ever.
      throw new IllegalArgumentException("Configuration error, expected "
          + "OzoneManager version config evaluates to a future version.");
    }
    // if expected version is unset or is the default, then any OM would do fine
    if (minimumVersion == null
        || minimumVersion == OzoneManagerVersion.DEFAULT_VERSION) {
      return true;
    }

    boolean found = false; // At min one OM should be present.
    for (ServiceInfo s: serviceInfoList) {
      if (s.getNodeType() == HddsProtos.NodeType.OM) {
        OzoneManagerVersion omv =
            OzoneManagerVersion
                .fromProtoValue(s.getProtobuf().getOMVersion());
        if (minimumVersion.compareTo(omv) > 0) {
          return false;
        } else {
          found = true;
        }
      }
    }
    return found;
  }

  @Nonnull
  @VisibleForTesting
  protected XceiverClientFactory createXceiverClientFactory(
      ServiceInfoEx serviceInfo) throws IOException {
    ClientTrustManager trustManager = null;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      CACertificateProvider remoteCAProvider =
          () -> ozoneManagerClient.getServiceInfo().provideCACerts();
      trustManager = new ClientTrustManager(remoteCAProvider, serviceInfo);
    }
    return new XceiverClientManager(conf,
        conf.getObject(XceiverClientManager.ScmClientConfig.class),
        trustManager);
  }

  @VisibleForTesting
  protected OmTransport createOmTransport(String omServiceId)
      throws IOException {
    return OmTransportFactory.create(conf, ugi, omServiceId);
  }

  @Override
  public List<OMRoleInfo> getOmRoleInfos() throws IOException {

    List<ServiceInfo> serviceList = ozoneManagerClient.getServiceList();
    List<OMRoleInfo> roleInfos = new ArrayList<>();

    for (ServiceInfo serviceInfo : serviceList) {
      if (serviceInfo.getNodeType().equals(HddsProtos.NodeType.OM)) {
        OMRoleInfo omRoleInfo = serviceInfo.getOmRoleInfo();
        if (omRoleInfo != null) {
          roleInfos.add(omRoleInfo);
        }
      }
    }
    return roleInfos;
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
    createVolume(volumeName, VolumeArgs.newBuilder().build());
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs volArgs)
      throws IOException {
    verifyVolumeName(volumeName);
    Preconditions.checkNotNull(volArgs);
    verifyCountsQuota(volArgs.getQuotaInNamespace());
    verifySpaceQuota(volArgs.getQuotaInBytes());

    String admin = volArgs.getAdmin() == null ?
        ugi.getShortUserName() : volArgs.getAdmin();
    String owner = volArgs.getOwner() == null ?
        ugi.getShortUserName() : volArgs.getOwner();
    long quotaInNamespace = volArgs.getQuotaInNamespace();
    long quotaInBytes = volArgs.getQuotaInBytes();

    OmVolumeArgs.Builder builder = OmVolumeArgs.newBuilder();
    builder.setVolume(volumeName);
    builder.setAdminName(admin);
    builder.setOwnerName(owner);
    builder.setQuotaInBytes(quotaInBytes);
    builder.setQuotaInNamespace(quotaInNamespace);
    builder.setUsedNamespace(0L);
    builder.addAllMetadata(volArgs.getMetadata());
    //ACLs from VolumeArgs
    List<OzoneAcl> volumeAcls = volArgs.getAcls();
    if (volumeAcls != null) {
      //Remove duplicates and add ACLs
      for (OzoneAcl ozoneAcl :
          volumeAcls.stream().distinct().collect(Collectors.toList())) {
        builder.addOzoneAcls(ozoneAcl);
      }
    }

    if (volArgs.getQuotaInBytes() == 0) {
      LOG.info("Creating Volume: {}, with {} as owner.", volumeName, owner);
    } else {
      LOG.info("Creating Volume: {}, with {} as owner "
              + "and space quota set to {} bytes, counts quota set" +
              " to {}", volumeName, owner, quotaInBytes, quotaInNamespace);
    }
    ozoneManagerClient.createVolume(builder.build());
  }

  @Override
  public boolean setVolumeOwner(String volumeName, String owner)
      throws IOException {
    verifyVolumeName(volumeName);
    Preconditions.checkNotNull(owner);
    return ozoneManagerClient.setOwner(volumeName, owner);
  }

  @Override
  public void setVolumeQuota(String volumeName, long quotaInNamespace,
      long quotaInBytes) throws IOException {
    verifyVolumeName(volumeName);
    verifyCountsQuota(quotaInNamespace);
    verifySpaceQuota(quotaInBytes);
    // If the volume is old, we need to remind the user on the client side
    // that it is not recommended to enable quota.
    OmVolumeArgs omVolumeArgs = ozoneManagerClient.getVolumeInfo(volumeName);
    if (omVolumeArgs.getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
      LOG.warn("Volume {} is created before version 1.1.0, usedNamespace " +
          "may be inaccurate and it is not recommended to enable quota.",
          volumeName);
    }
    ozoneManagerClient.setQuota(volumeName, quotaInNamespace, quotaInBytes);
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName)
      throws IOException {
    verifyVolumeName(volumeName);
    OmVolumeArgs volume = ozoneManagerClient.getVolumeInfo(volumeName);
    return buildOzoneVolume(volume);
  }

  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {
    S3VolumeContext resp = ozoneManagerClient.getS3VolumeContext();
    String userPrincipal = resp.getUserPrincipal();
    updateS3Principal(userPrincipal);
    return resp;
  }

  private void updateS3Principal(String userPrincipal) {
    S3Auth s3Auth = this.getThreadLocalS3Auth();
    // Update user principal if needed to be used for KMS client
    if (s3Auth != null) {
      // Update userPrincipal field with the value returned from OM. So that
      //  in multi-tenancy, KMS client can use the correct identity
      //  (instead of using accessId) to communicate with KMS.
      LOG.debug("Updating S3Auth.userPrincipal to {}", userPrincipal);
      s3Auth.setUserPrincipal(userPrincipal);
      this.setThreadLocalS3Auth(s3Auth);
    }
  }

  @Override
  public OzoneVolume buildOzoneVolume(OmVolumeArgs volume) {
    return OzoneVolume.newBuilder(conf, this)
        .setName(volume.getVolume())
        .setAdmin(volume.getAdminName())
        .setOwner(volume.getOwnerName())
        .setQuotaInBytes(volume.getQuotaInBytes())
        .setQuotaInNamespace(volume.getQuotaInNamespace())
        .setUsedNamespace(volume.getUsedNamespace())
        .setCreationTime(volume.getCreationTime())
        .setModificationTime(volume.getModificationTime())
        .setAcls(volume.getAcls())
        .setMetadata(volume.getMetadata())
        .setRefCount(volume.getRefCount())
        .build();
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    verifyVolumeName(volumeName);
    ozoneManagerClient.deleteVolume(volumeName);
  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume,
                                       int maxListResult)
      throws IOException {
    List<OmVolumeArgs> volumes = ozoneManagerClient.listAllVolumes(
        volumePrefix, prevVolume, maxListResult);

    return volumes.stream().map(volume ->
            OzoneVolume.newBuilder(conf, this)
                .setName(volume.getVolume())
                .setAdmin(volume.getAdminName())
                .setOwner(volume.getOwnerName())
                .setQuotaInBytes(volume.getQuotaInBytes())
                .setQuotaInNamespace(volume.getQuotaInNamespace())
                .setUsedNamespace(volume.getUsedNamespace())
                .setCreationTime(volume.getCreationTime())
                .setModificationTime(volume.getModificationTime())
                .setAcls(volume.getAcls())
                .build())
        .collect(Collectors.toList());
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                       String prevVolume, int maxListResult)
      throws IOException {
    List<OmVolumeArgs> volumes = ozoneManagerClient.listVolumeByUser(
        user, volumePrefix, prevVolume, maxListResult);

    return volumes.stream().map(volume ->
            OzoneVolume.newBuilder(conf, this)
                .setName(volume.getVolume())
                .setAdmin(volume.getAdminName())
                .setOwner(volume.getOwnerName())
                .setQuotaInBytes(volume.getQuotaInBytes())
                .setQuotaInNamespace(volume.getQuotaInNamespace())
                .setUsedNamespace(volume.getUsedNamespace())
                .setCreationTime(volume.getCreationTime())
                .setModificationTime(volume.getModificationTime())
                .setAcls(volume.getAcls())
                .setMetadata(volume.getMetadata())
                .build())
        .collect(Collectors.toList());
  }

  @Override
  public void createBucket(String volumeName, String bucketName)
      throws IOException {
    // Set acls of current user.
    createBucket(volumeName, bucketName,
        BucketArgs.newBuilder().build());
  }

  @Override
  public void createBucket(
      String volumeName, String bucketName, BucketArgs bucketArgs)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(bucketArgs);
    verifyCountsQuota(bucketArgs.getQuotaInNamespace());
    verifySpaceQuota(bucketArgs.getQuotaInBytes());
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (bucketArgs.getDefaultReplicationConfig() != null &&
          bucketArgs.getDefaultReplicationConfig().getType()
          == ReplicationType.EC) {
        throw new IOException("Can not set the default replication of the"
            + " bucket to Erasure Coded replication, as OzoneManager does"
            + " not support Erasure Coded replication.");
      }
    }

    final String owner;
    // If S3 auth exists, set owner name to the short user name derived from the
    //  accessId. Similar to RpcClient#getDEK
    if (getThreadLocalS3Auth() != null) {
      final UserGroupInformation s3gUGI = UserGroupInformation.createRemoteUser(
          getThreadLocalS3Auth().getUserPrincipal());
      owner = s3gUGI.getShortUserName();
    } else {
      owner = bucketArgs.getOwner() == null ?
          ugi.getShortUserName() : bucketArgs.getOwner();
    }

    boolean isVersionEnabled = bucketArgs.getVersioning();
    StorageType storageType = bucketArgs.getStorageType() == null ?
        StorageType.DEFAULT : bucketArgs.getStorageType();
    BucketLayout bucketLayout = bucketArgs.getBucketLayout();
    BucketEncryptionKeyInfo bek = null;
    if (bucketArgs.getEncryptionKey() != null) {
      bek = new BucketEncryptionKeyInfo.Builder()
          .setKeyName(bucketArgs.getEncryptionKey()).build();
    }

    OmBucketInfo.Builder builder = OmBucketInfo.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setIsVersionEnabled(isVersionEnabled)
        .addAllMetadata(bucketArgs.getMetadata())
        .setStorageType(storageType)
        .setSourceVolume(bucketArgs.getSourceVolume())
        .setSourceBucket(bucketArgs.getSourceBucket())
        .setQuotaInBytes(bucketArgs.getQuotaInBytes())
        .setQuotaInNamespace(bucketArgs.getQuotaInNamespace())
        .setBucketLayout(bucketLayout)
        .setOwner(owner);

    if (bucketArgs.getAcls() != null) {
      builder.setAcls(bucketArgs.getAcls());
    }

    // Link bucket default acl
    if (bucketArgs.getSourceVolume() != null
        && bucketArgs.getSourceBucket() != null) {
      builder.addAcl(LINK_BUCKET_DEFAULT_ACL);
    }

    if (bek != null) {
      builder.setBucketEncryptionKey(bek);
    }

    DefaultReplicationConfig defaultReplicationConfig =
        bucketArgs.getDefaultReplicationConfig();
    if (defaultReplicationConfig != null) {
      builder.setDefaultReplicationConfig(defaultReplicationConfig);
    }

    String replicationType = defaultReplicationConfig == null 
        ? "server-side default replication type"
        : defaultReplicationConfig.getType().toString();

    String layoutMsg = bucketLayout != null
        ? "with bucket layout " + bucketLayout
        : "with server-side default bucket layout";
    LOG.info("Creating Bucket: {}/{}, {}, {} as owner, Versioning {}, " +
            "Storage Type set to {} and Encryption set to {}, " +
            "Replication Type set to {}, Namespace Quota set to {}, " +
            "Space Quota set to {} ",
        volumeName, bucketName, layoutMsg, owner, isVersionEnabled,
        storageType, bek != null, replicationType,
        bucketArgs.getQuotaInNamespace(), bucketArgs.getQuotaInBytes());

    ozoneManagerClient.createBucket(builder.build());
  }

  private static void verifyVolumeName(String volumeName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(volumeName, "volume", false);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }
  }

  private static void verifyBucketName(String bucketName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(bucketName, "bucket", false);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_BUCKET_NAME);
    }
  }

  private static void verifyCountsQuota(long quota) throws OMException {
    if (quota < OzoneConsts.QUOTA_RESET || quota == 0) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "counts quota is :" + quota + ".");
    }
  }

  private static void verifySpaceQuota(long quota) throws OMException {
    if (quota < OzoneConsts.QUOTA_RESET || quota == 0) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "space quota is :" + quota + ".");
    }
  }

  /**
   * Helper function to get the actual operating user.
   *
   * @return listOfAcls
   * */
  private UserGroupInformation getRealUserInfo() {
    // After HDDS-5881 the user will not be different,
    // as S3G uses single RpcClient. So we should be checking thread-local
    // S3Auth and use it during proxy.
    if (ozoneManagerClient.getThreadLocalS3Auth() != null) {
      return s3gUgi;
    }
    return ugi;
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {

    Token<OzoneTokenIdentifier> token =
        ozoneManagerClient.getDelegationToken(renewer);
    if (token != null) {
      token.setService(dtService);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created token {} for dtService {}", token, dtService);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot get ozone delegation token for renewer {} to " +
            "access service {}", renewer, dtService);
      }
    }
    return token;
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return ozoneManagerClient.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    ozoneManagerClient.cancelDelegationToken(token);
  }

  /**
   * Returns s3 secret given a kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  @Override
  @Nonnull
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(kerberosID),
        "kerberosID cannot be null or empty.");

    return ozoneManagerClient.getS3Secret(kerberosID);
  }

  /**
   * Returns s3 secret given a kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  @Override
  public S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist)
          throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(kerberosID),
            "kerberosID cannot be null or empty.");
    // No need to check createIfNotExist here which is a primitive
    return ozoneManagerClient.getS3Secret(kerberosID, createIfNotExist);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public S3SecretValue setS3Secret(String accessId, String secretKey)
          throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessId),
            "accessId cannot be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(secretKey),
            "secretKey cannot be null or empty.");
    return ozoneManagerClient.setS3Secret(accessId, secretKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void revokeS3Secret(String kerberosID) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(kerberosID),
            "kerberosID cannot be null or empty.");

    ozoneManagerClient.revokeS3Secret(kerberosID);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTenant(String tenantId) throws IOException {
    createTenant(tenantId, TenantArgs.newBuilder()
        .setVolumeName(tenantId).build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTenant(String tenantId, TenantArgs tenantArgs)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(tenantId),
        "tenantId cannot be null or empty.");
    Preconditions.checkNotNull(tenantArgs);

    final String volumeName = tenantArgs.getVolumeName();
    verifyVolumeName(volumeName);

    final boolean forceCreationWhenVolumeExists =
        tenantArgs.getForceCreationWhenVolumeExists();

    OmTenantArgs.Builder builder = OmTenantArgs.newBuilder();
    builder.setTenantId(tenantId);
    builder.setVolumeName(volumeName);
    builder.setForceCreationWhenVolumeExists(
        tenantArgs.getForceCreationWhenVolumeExists());

    // TODO: Add more fields. e.g. include OmVolumeArgs in (Om)TenantArgs
    //  as well for customized volume creation.

    LOG.info("Creating Tenant: '{}', with volume: '{}', "
            + "forceCreationWhenVolumeExists: {}",
        tenantId, volumeName, forceCreationWhenVolumeExists);

    ozoneManagerClient.createTenant(builder.build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DeleteTenantState deleteTenant(String tenantId) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(tenantId),
        "tenantId cannot be null or empty.");
    return ozoneManagerClient.deleteTenant(tenantId);
  }

  /**
   * Assign user to tenant.
   * @param username user name to be assigned.
   * @param tenantId tenant name.
   * @throws IOException
   */
  @Override
  public S3SecretValue tenantAssignUserAccessId(
      String username, String tenantId, String accessId) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(username),
        "username can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(tenantId),
        "tenantId can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(accessId),
        "accessId can't be null or empty.");
    Preconditions.checkArgument(
        accessId.length() <= OZONE_MAXIMUM_ACCESS_ID_LENGTH, "accessId length ("
            + accessId.length() + ") exceeds the maximum length allowed ("
            + OZONE_MAXIMUM_ACCESS_ID_LENGTH + ")");
    return ozoneManagerClient.tenantAssignUserAccessId(
        username, tenantId, accessId);
  }

  /**
   * Revoke user accessId to tenant.
   * @param accessId accessId to be revoked.
   * @throws IOException
   */
  @Override
  public void tenantRevokeUserAccessId(String accessId) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessId),
        "accessId can't be null or empty.");
    ozoneManagerClient.tenantRevokeUserAccessId(accessId);
  }

  /**
   * Create Snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name to be used
   * @return name used
   * @throws IOException
   */
  @Override
  public String createSnapshot(String volumeName,
      String bucketName, String snapshotName) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    return ozoneManagerClient.createSnapshot(volumeName,
        bucketName, snapshotName);
  }

  /**
   * Rename Snapshot.
   *
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   *
   * @throws IOException
   */
  @Override
  public void renameSnapshot(String volumeName,
      String bucketName, String snapshotOldName, String snapshotNewName) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
                                "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
                                "bucket can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(snapshotOldName),
                                "old snapshot name can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(snapshotNewName),
                                "new snapshot name can't be null or empty.");

    ozoneManagerClient.renameSnapshot(volumeName, bucketName, snapshotOldName, snapshotNewName);
  }

  /**
   * Delete Snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name of the snapshot to be deleted
   * @throws IOException
   */
  @Override
  public void deleteSnapshot(String volumeName,
      String bucketName, String snapshotName) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(snapshotName),
        "snapshot name can't be null or empty.");
    ozoneManagerClient.deleteSnapshot(volumeName, bucketName, snapshotName);
  }

  /**
   * Returns snapshot info for volume/bucket snapshot path.
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param snapshotName snapshot name
   * @return snapshot info for volume/bucket snapshot path.
   * @throws IOException
   */
  @Override
  public OzoneSnapshot getSnapshotInfo(String volumeName,
                                       String bucketName,
                                       String snapshotName) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(snapshotName),
        "snapshot name can't be null or empty.");
    SnapshotInfo snapshotInfo = ozoneManagerClient.getSnapshotInfo(volumeName,
        bucketName, snapshotName);
    return OzoneSnapshot.fromSnapshotInfo(snapshotInfo);
  }

  /**
   * Create an image of the current compaction log DAG in the OM.
   * @param fileNamePrefix  file name prefix of the image file.
   * @param graphType       type of node name to use in the graph image.
   * @return message which tells the image name, parent dir and OM leader
   * node information.
   */
  @Deprecated
  @Override
  public String printCompactionLogDag(String fileNamePrefix,
                                      String graphType) throws IOException {
    return ozoneManagerClient.printCompactionLogDag(fileNamePrefix, graphType);
  }

  @Override
  public SnapshotDiffResponse snapshotDiff(String volumeName,
                                           String bucketName,
                                           String fromSnapshot,
                                           String toSnapshot,
                                           String token,
                                           int pageSize,
                                           boolean forceFullDiff,
                                           boolean disableNativeDiff)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    return ozoneManagerClient.snapshotDiff(volumeName, bucketName,
        fromSnapshot, toSnapshot, token, pageSize, forceFullDiff,
        disableNativeDiff);
  }

  @Override
  public CancelSnapshotDiffResponse cancelSnapshotDiff(String volumeName,
                                                       String bucketName,
                                                       String fromSnapshot,
                                                       String toSnapshot)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(fromSnapshot),
        "fromSnapshot can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(toSnapshot),
        "toSnapshot can't be null or empty.");
    return ozoneManagerClient.cancelSnapshotDiff(volumeName, bucketName,
        fromSnapshot, toSnapshot);
  }

  @Override
  public ListSnapshotDiffJobResponse listSnapshotDiffJobs(
      String volumeName,
      String bucketName,
      String jobStatus,
      boolean listAllStatus,
      String prevSnapshotDiffJob,
      int maxListResult) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    return ozoneManagerClient.listSnapshotDiffJobs(volumeName, bucketName, jobStatus, listAllStatus,
        prevSnapshotDiffJob, maxListResult);
  }

  /**
   * List snapshots in a volume/bucket.
   * @param volumeName     volume name
   * @param bucketName     bucket name
   * @param snapshotPrefix snapshot prefix to match
   * @param prevSnapshot   snapshots will be listed after this snapshot name
   * @param maxListResult  max number of snapshots to return
   * @return list of snapshots for volume/bucket path.
   * @throws IOException
   */
  @Override
  public ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(volumeName),
        "volume can't be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName),
        "bucket can't be null or empty.");
    return ozoneManagerClient.listSnapshot(volumeName, bucketName, snapshotPrefix, prevSnapshot, maxListResult);
  }

  /**
   * Assign admin role to an accessId in a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @param delegated true if making delegated admin.
   * @throws IOException
   */
  @Override
  public void tenantAssignAdmin(String accessId, String tenantId,
      boolean delegated)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessId),
        "accessId can't be null or empty.");
    // tenantId can be empty
    ozoneManagerClient.tenantAssignAdmin(accessId, tenantId, delegated);
  }

  /**
   * Revoke admin role of an accessId from a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @throws IOException
   */
  @Override
  public void tenantRevokeAdmin(String accessId, String tenantId)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessId),
        "accessId can't be null or empty.");
    // tenantId can be empty
    ozoneManagerClient.tenantRevokeAdmin(accessId, tenantId);
  }

  /**
   * Get tenant info for a user.
   * @param userPrincipal Kerberos principal of a user.
   * @return TenantUserInfo
   * @throws IOException
   */
  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(userPrincipal),
        "userPrincipal can't be null or empty.");
    return ozoneManagerClient.tenantGetUserInfo(userPrincipal);
  }

  /**
   * List tenants.
   * @return TenantStateList
   * @throws IOException
   */
  @Override
  public TenantStateList listTenant() throws IOException {
    return ozoneManagerClient.listTenant();
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException {
    return ozoneManagerClient.listUsersInTenant(tenantId, prefix);
  }

  @Override
  public void setBucketVersioning(
      String volumeName, String bucketName, Boolean versioning)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(versioning);
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setIsVersionEnabled(versioning);
    ozoneManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void setBucketStorageType(
      String volumeName, String bucketName, StorageType storageType)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(storageType);
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(storageType);
    ozoneManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void setBucketQuota(String volumeName, String bucketName,
      long quotaInNamespace, long quotaInBytes) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    verifyCountsQuota(quotaInNamespace);
    verifySpaceQuota(quotaInBytes);
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace);
    // If the bucket is old, we need to remind the user on the client side
    // that it is not recommended to enable quota.
    OmBucketInfo omBucketInfo = ozoneManagerClient.getBucketInfo(
        volumeName, bucketName);
    if (omBucketInfo.getQuotaInNamespace() == OLD_QUOTA_DEFAULT ||
        omBucketInfo.getUsedBytes() == OLD_QUOTA_DEFAULT) {
      LOG.warn("Bucket {} is created before version 1.1.0, usedBytes or " +
          "usedNamespace may be inaccurate and it is not recommended to " +
          "enable quota.", bucketName);
    }
    ozoneManagerClient.setBucketProperty(builder.build());

  }

  @Deprecated
  @Override
  public void setEncryptionKey(String volumeName, String bucketName,
                               String bekName) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    BucketEncryptionKeyInfo bek = new BucketEncryptionKeyInfo.Builder()
        .setKeyName(bekName).build();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketEncryptionKey(bek);
    OmBucketArgs finalArgs = builder.build();
    ozoneManagerClient.setBucketProperty(finalArgs);
  }

  @Override
  public void setReplicationConfig(
      String volumeName, String bucketName, ReplicationConfig replicationConfig)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(replicationConfig);
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (replicationConfig.getReplicationType()
          == HddsProtos.ReplicationType.EC) {
        throw new IOException("Can not set the default replication of the"
            + " bucket to Erasure Coded replication, as OzoneManager does"
            + " not support Erasure Coded replication.");
      }
    }
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(replicationConfig));
    ozoneManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void deleteBucket(
      String volumeName, String bucketName) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    ozoneManagerClient.deleteBucket(volumeName, bucketName);
  }

  @Override
  public void checkBucketAccess(
      String volumeName, String bucketName) throws IOException {

  }

  @Override
  public OzoneBucket getBucketDetails(
      String volumeName, String bucketName) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    OmBucketInfo bucketInfo =
        ozoneManagerClient.getBucketInfo(volumeName, bucketName);
    return OzoneBucket.newBuilder(conf, this)
        .setVolumeName(bucketInfo.getVolumeName())
        .setName(bucketInfo.getBucketName())
        .setStorageType(bucketInfo.getStorageType())
        .setVersioning(bucketInfo.getIsVersionEnabled())
        .setCreationTime(bucketInfo.getCreationTime())
        .setModificationTime(bucketInfo.getModificationTime())
        .setMetadata(bucketInfo.getMetadata())
        .setEncryptionKeyName(bucketInfo.getEncryptionKeyInfo() != null ?
            bucketInfo.getEncryptionKeyInfo().getKeyName() : null)
        .setSourceVolume(bucketInfo.getSourceVolume())
        .setSourceBucket(bucketInfo.getSourceBucket())
        .setUsedBytes(bucketInfo.getTotalBucketSpace())
        .setUsedNamespace(bucketInfo.getTotalBucketNamespace())
        .setQuotaInBytes(bucketInfo.getQuotaInBytes())
        .setQuotaInNamespace(bucketInfo.getQuotaInNamespace())
        .setBucketLayout(bucketInfo.getBucketLayout())
        .setOwner(bucketInfo.getOwner())
        .setDefaultReplicationConfig(bucketInfo.getDefaultReplicationConfig())
        .build();
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                       String prevBucket, int maxListResult,
                                       boolean hasSnapshot)
      throws IOException {
    List<OmBucketInfo> buckets = ozoneManagerClient.listBuckets(
        volumeName, prevBucket, bucketPrefix, maxListResult, hasSnapshot);

    return buckets.stream().map(bucket -> 
            OzoneBucket.newBuilder(conf, this)
                .setVolumeName(bucket.getVolumeName())
                .setName(bucket.getBucketName())
                .setStorageType(bucket.getStorageType())
                .setVersioning(bucket.getIsVersionEnabled())
                .setCreationTime(bucket.getCreationTime())
                .setModificationTime(bucket.getModificationTime())
                .setMetadata(bucket.getMetadata())
                .setEncryptionKeyName(bucket.getEncryptionKeyInfo() != null ?
                    bucket.getEncryptionKeyInfo().getKeyName() : null)
                .setSourceVolume(bucket.getSourceVolume())
                .setSourceBucket(bucket.getSourceBucket())
                .setUsedBytes(bucket.getTotalBucketSpace())
                .setUsedNamespace(bucket.getTotalBucketNamespace())
                .setQuotaInBytes(bucket.getQuotaInBytes())
                .setQuotaInNamespace(bucket.getQuotaInNamespace())
                .setBucketLayout(bucket.getBucketLayout())
                .setOwner(bucket.getOwner())
                .setDefaultReplicationConfig(
                    bucket.getDefaultReplicationConfig())
                .build())
        .collect(Collectors.toList());
  }

  @Override
  @Deprecated
  public OzoneOutputStream createKey(String volumeName, String bucketName,
      String keyName, long size, ReplicationType type, ReplicationFactor factor,
      Map<String, String> metadata) throws IOException {

    return createKey(volumeName, bucketName, keyName, size,
        ReplicationConfig.fromTypeAndFactor(type, factor), metadata);
  }

  @Override
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException {
    return createKey(volumeName, bucketName, keyName, size, replicationConfig,
        metadata, Collections.emptyMap());
  }

  @Override
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags) throws IOException {
    createKeyPreChecks(volumeName, bucketName, keyName, replicationConfig);

    if (omVersion.compareTo(OzoneManagerVersion.OBJECT_TAG) < 0) {
      if (tags != null && !tags.isEmpty()) {
        throw new IOException("OzoneManager does not support object tags");
      }
    }

    String ownerName = getRealUserInfo().getShortUserName();

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .addAllMetadataGdpr(metadata)
        .addAllTags(tags)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setOwnerName(ownerName);

    OpenKeySession openKey = ozoneManagerClient.openKey(builder.build());
    // For bucket with layout OBJECT_STORE, when create an empty file (size=0),
    // OM will set DataSize to OzoneConfigKeys#OZONE_SCM_BLOCK_SIZE,
    // which will cause S3G's atomic write length check to fail,
    // so reset size to 0 here.
    if (isS3GRequest.get() && size == 0) {
      openKey.getKeyInfo().setDataSize(size);
    }
    return createOutputStream(openKey);
  }

  @Override
  public OzoneOutputStream rewriteKey(String volumeName, String bucketName, String keyName,
      long size, long existingKeyGeneration, ReplicationConfig replicationConfig,
      Map<String, String> metadata) throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.ATOMIC_REWRITE_KEY) < 0) {
      throw new IOException("OzoneManager does not support atomic key rewrite.");
    }

    createKeyPreChecks(volumeName, bucketName, keyName, replicationConfig);

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .addAllMetadataGdpr(metadata)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setExpectedDataGeneration(existingKeyGeneration);

    OpenKeySession openKey = ozoneManagerClient.openKey(builder.build());
    // For bucket with layout OBJECT_STORE, when create an empty file (size=0),
    // OM will set DataSize to OzoneConfigKeys#OZONE_SCM_BLOCK_SIZE,
    // which will cause S3G's atomic write length check to fail,
    // so reset size to 0 here.
    if (isS3GRequest.get() && size == 0) {
      openKey.getKeyInfo().setDataSize(0);
    }
    return createOutputStream(openKey);
  }

  private void createKeyPreChecks(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName);
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (replicationConfig != null &&
          replicationConfig.getReplicationType()
              == HddsProtos.ReplicationType.EC) {
        throw new IOException("Can not set the replication of the key to"
            + " Erasure Coded replication, as OzoneManager does not support"
            + " Erasure Coded replication.");
      }
    }

    if (replicationConfig != null) {
      replicationConfigValidator.validate(replicationConfig);
    }
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException {
    return createStreamKey(volumeName, bucketName, keyName, size, replicationConfig,
        metadata, Collections.emptyMap());
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName);

    if (omVersion.compareTo(OzoneManagerVersion.OBJECT_TAG) < 0) {
      if (tags != null && !tags.isEmpty()) {
        throw new IOException("OzoneManager does not support object tags");
      }
    }

    String ownerName = getRealUserInfo().getShortUserName();

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .addAllMetadataGdpr(metadata)
        .addAllTags(tags)
        .setSortDatanodesInPipeline(true)
        .setOwnerName(ownerName);

    OpenKeySession openKey = ozoneManagerClient.openKey(builder.build());
    return createDataStreamOutput(openKey);
  }

  private KeyProvider.KeyVersion getDEK(FileEncryptionInfo feInfo)
      throws IOException {
    // check crypto protocol version
    OzoneKMSUtil.checkCryptoProtocolVersion(feInfo);
    KeyProvider.KeyVersion decrypted = null;
    try {

      // After HDDS-5881 the user will not be different,
      // as S3G uses single RpcClient. So we should be checking thread-local
      // S3Auth and use it during proxy.
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation proxyUser;
      if (getThreadLocalS3Auth() != null) {
        String userPrincipal = getThreadLocalS3Auth().getUserPrincipal();
        Preconditions.checkNotNull(userPrincipal);
        UserGroupInformation s3gUGI = UserGroupInformation.createRemoteUser(
            userPrincipal);
        proxyUser = UserGroupInformation.createProxyUser(
            s3gUGI.getShortUserName(), loginUser);
        decrypted = proxyUser.doAs(
            (PrivilegedExceptionAction<KeyProvider.KeyVersion>) () -> {
              return OzoneKMSUtil.decryptEncryptedDataEncryptionKey(feInfo,
                  getKeyProvider());
            });
      } else {
        decrypted = OzoneKMSUtil.decryptEncryptedDataEncryptionKey(feInfo,
            getKeyProvider());
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during decrypt key", ex);
    }
    return decrypted;
  }

  @Override
  public OzoneInputStream getKey(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyInfo keyInfo = getKeyInfo(volumeName, bucketName, keyName, false);
    return getInputStreamWithRetryFunction(keyInfo);
  }

  /**
   * Returns a map that contains {@link org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo} objects of the given key
   * as keys of the map.
   * Values of the returned map are internal blocks of {@link org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo}.
   * The blocks are represented by another map of {@link org.apache.hadoop.hdds.protocol.DatanodeDetails} as keys and
   * {@link org.apache.hadoop.ozone.client.io.OzoneInputStream} as values.
   * replicaitonConfig in dnKeyInfo is used to instantiate an input stream for each block.
   * In case of EC, ECBlockInputStream is instantiated and causes an error later when the stream is processed.
   * To prevent such an error, RATIS ONE replication is used instead and the length of each block is calculated by a
   * helper method.
   */
  @Override
  public Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream> >
      getKeysEveryReplicas(String volumeName,
                         String bucketName,
                         String keyName) throws IOException {

    Map< OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream> > result
        = new LinkedHashMap<>();

    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    OmKeyInfo keyInfo = getKeyInfo(volumeName, bucketName, keyName, true);
    List<OmKeyLocationInfo> keyLocationInfos
        = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    ReplicationConfig replicationConfig = keyInfo.getReplicationConfig();

    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      Map<DatanodeDetails, OzoneInputStream> blocks = new HashMap<>();


      Pipeline pipelineBefore = locationInfo.getPipeline();
      List<DatanodeDetails> datanodes = pipelineBefore.getNodes();

      for (DatanodeDetails dn : datanodes) {
        Pipeline pipeline = pipelineBefore.copyForReadFromNode(dn);
        long length = replicationConfig instanceof ECReplicationConfig
                ? ECBlockInputStream.internalBlockLength(pipelineBefore.getReplicaIndex(dn),
                (ECReplicationConfig) replicationConfig, locationInfo.getLength())
                : locationInfo.getLength();
        OmKeyLocationInfo dnKeyLocation = new OmKeyLocationInfo.Builder()
            .setBlockID(locationInfo.getBlockID())
            .setLength(length)
            .setOffset(locationInfo.getOffset())
            .setToken(locationInfo.getToken())
            .setPartNumber(locationInfo.getPartNumber())
            .setCreateVersion(locationInfo.getCreateVersion())
            .setPipeline(pipeline)
            .build();

        List<OmKeyLocationInfo> keyLocationInfoList =
            Collections.singletonList(dnKeyLocation);
        OmKeyLocationInfoGroup keyLocationInfoGroup
            = new OmKeyLocationInfoGroup(0, keyLocationInfoList);
        List<OmKeyLocationInfoGroup> keyLocationInfoGroups =
            Collections.singletonList(keyLocationInfoGroup);

        keyInfo.setKeyLocationVersions(keyLocationInfoGroups);
        OmKeyInfo dnKeyInfo = new OmKeyInfo.Builder()
            .setVolumeName(keyInfo.getVolumeName())
            .setBucketName(keyInfo.getBucketName())
            .setKeyName(keyInfo.getKeyName())
            .setOmKeyLocationInfos(keyInfo.getKeyLocationVersions())
            .setDataSize(keyInfo.getDataSize())
            .setCreationTime(keyInfo.getCreationTime())
            .setModificationTime(keyInfo.getModificationTime())
            .setReplicationConfig(replicationConfig instanceof ECReplicationConfig
                    ? RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE)
                    : keyInfo.getReplicationConfig())
            .setFileEncryptionInfo(keyInfo.getFileEncryptionInfo())
            .setAcls(keyInfo.getAcls())
            .setObjectID(keyInfo.getObjectID())
            .setUpdateID(keyInfo.getUpdateID())
            .setParentObjectID(keyInfo.getParentObjectID())
            .setFileChecksum(keyInfo.getFileChecksum())
            .setOwnerName(keyInfo.getOwnerName())
            .build();
        dnKeyInfo.setMetadata(keyInfo.getMetadata());
        dnKeyInfo.setKeyLocationVersions(keyLocationInfoGroups);

        blocks.put(dn, createInputStream(dnKeyInfo, Function.identity()));
      }

      result.put(locationInfo, blocks);
    }

    return result;
  }

  @Override
  public void deleteKey(
      String volumeName, String bucketName, String keyName, boolean recursive)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRecursive(recursive)
        .build();
    ozoneManagerClient.deleteKey(keyArgs);
  }

  @Override
  public void deleteKeys(
          String volumeName, String bucketName, List<String> keyNameList)
          throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyNameList);
    OmDeleteKeys omDeleteKeys = new OmDeleteKeys(volumeName, bucketName,
        keyNameList);
    ozoneManagerClient.deleteKeys(omDeleteKeys);
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(
      String volumeName, String bucketName, List<String> keyNameList, boolean quiet)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyNameList);
    OmDeleteKeys omDeleteKeys = new OmDeleteKeys(volumeName, bucketName,
        keyNameList);
    return ozoneManagerClient.deleteKeys(omDeleteKeys, quiet);
  }

  @Override
  public void renameKey(String volumeName, String bucketName,
      String fromKeyName, String toKeyName) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(toKeyName);
    }
    HddsClientUtils.checkNotNull(fromKeyName, toKeyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(fromKeyName)
        .build();
    ozoneManagerClient.renameKey(keyArgs, toKeyName);
  }

  @Override
  @Deprecated
  public void renameKeys(String volumeName, String bucketName,
                         Map<String, String> keyMap) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyMap);
    OmRenameKeys omRenameKeys =
        new OmRenameKeys(volumeName, bucketName, keyMap, null);
    ozoneManagerClient.renameKeys(omRenameKeys);
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
                                 String keyPrefix, String prevKey,
                                 int maxListResult)
      throws IOException {

    if (omVersion.compareTo(OzoneManagerVersion.LIGHTWEIGHT_LIST_KEYS) >= 0) {
      List<BasicOmKeyInfo> keys = ozoneManagerClient.listKeysLight(
          volumeName, bucketName, prevKey, keyPrefix, maxListResult).getKeys();

      return keys.stream().map(key -> new OzoneKey(
              key.getVolumeName(),
              key.getBucketName(),
              key.getKeyName(),
              key.getDataSize(),
              key.getCreationTime(),
              key.getModificationTime(),
              key.getReplicationConfig(),
              Collections.singletonMap(ETAG, key.getETag()),
              key.isFile(),
              key.getOwnerName(),
              Collections.emptyMap()))
          .collect(Collectors.toList());
    } else {
      List<OmKeyInfo> keys = ozoneManagerClient.listKeys(
          volumeName, bucketName, prevKey, keyPrefix, maxListResult).getKeys();
      return keys.stream().map(key -> new OzoneKey(key.getVolumeName(),
              key.getBucketName(),
              key.getKeyName(),
              key.getDataSize(),
              key.getCreationTime(),
              key.getModificationTime(),
              key.getReplicationConfig(),
              key.getMetadata(),
              key.isFile(),
              key.getOwnerName(),
              key.getTags()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public OzoneKeyDetails getKeyDetails(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    OmKeyInfo keyInfo =
        getKeyInfo(volumeName, bucketName, keyName, false);
    return getOzoneKeyDetails(keyInfo);
  }

  @Nonnull
  private OzoneKeyDetails getOzoneKeyDetails(OmKeyInfo keyInfo) {
    List<OzoneKeyLocation> ozoneKeyLocations = new ArrayList<>();
    long lastKeyOffset = 0L;
    List<OmKeyLocationInfo> omKeyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();
    for (OmKeyLocationInfo info: omKeyLocationInfos) {
      ozoneKeyLocations.add(new OzoneKeyLocation(info.getContainerID(),
          info.getLocalID(), info.getLength(), info.getOffset(),
          lastKeyOffset));
      lastKeyOffset += info.getLength();
    }

    return new OzoneKeyDetails(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), ozoneKeyLocations,
        keyInfo.getReplicationConfig(), keyInfo.getMetadata(),
        keyInfo.getFileEncryptionInfo(),
        () -> getInputStreamWithRetryFunction(keyInfo), keyInfo.isFile(),
        keyInfo.getOwnerName(), keyInfo.getTags(),
        keyInfo.getGeneration()
    );
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName)
      throws IOException {
    OmKeyInfo keyInfo = getS3KeyInfo(bucketName, keyName, false);
    return getOzoneKeyDetails(keyInfo);
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName,
                                         int partNumber) throws IOException {
    OmKeyInfo keyInfo;
    if (omVersion.compareTo(OzoneManagerVersion.S3_PART_AWARE_GET) >= 0) {
      keyInfo = getS3PartKeyInfo(bucketName, keyName, partNumber);
    } else {
      keyInfo = getS3KeyInfo(bucketName, keyName, false);
      List<OmKeyLocationInfo> filteredKeyLocationInfo = keyInfo
          .getLatestVersionLocations().getBlocksLatestVersionOnly().stream()
          .filter(omKeyLocationInfo -> omKeyLocationInfo.getPartNumber() ==
                                       partNumber)
          .collect(Collectors.toList());
      keyInfo.updateLocationInfoList(filteredKeyLocationInfo, true, true);
      keyInfo.setDataSize(filteredKeyLocationInfo.stream()
          .mapToLong(OmKeyLocationInfo::getLength)
          .sum());
    }
    return getOzoneKeyDetails(keyInfo);
  }

  @Nonnull
  private OmKeyInfo getS3KeyInfo(
      String bucketName, String keyName, boolean isHeadOp) throws IOException {
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        // Volume name is not important, as we call GetKeyInfo with
        // assumeS3Context = true, OM will infer the correct s3 volume.
        .setVolumeName(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setForceUpdateContainerCacheFromSCM(false)
        .setHeadOp(isHeadOp)
        .build();
    KeyInfoWithVolumeContext keyInfoWithS3Context =
        ozoneManagerClient.getKeyInfo(keyArgs, true);
    keyInfoWithS3Context.getUserPrincipal().ifPresent(this::updateS3Principal);
    return keyInfoWithS3Context.getKeyInfo();
  }

  @Nonnull
  private OmKeyInfo getS3PartKeyInfo(
      String bucketName, String keyName, int partNumber) throws IOException {
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        // Volume name is not important, as we call GetKeyInfo with
        // assumeS3Context = true, OM will infer the correct s3 volume.
        .setVolumeName(OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setForceUpdateContainerCacheFromSCM(false)
        .setMultipartUploadPartNumber(partNumber)
        .build();
    KeyInfoWithVolumeContext keyInfoWithS3Context =
        ozoneManagerClient.getKeyInfo(keyArgs, true);
    keyInfoWithS3Context.getUserPrincipal().ifPresent(this::updateS3Principal);
    return keyInfoWithS3Context.getKeyInfo();
  }

  @Override
  public OmKeyInfo getKeyInfo(
      String volumeName, String bucketName, String keyName,
      boolean forceUpdateContainerCache) throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setForceUpdateContainerCacheFromSCM(forceUpdateContainerCache)
        .build();
    return getKeyInfo(keyArgs);
  }

  private OmKeyInfo getKeyInfo(OmKeyArgs keyArgs) throws IOException {
    final OmKeyInfo keyInfo;
    if (omVersion.compareTo(OzoneManagerVersion.OPTIMIZED_GET_KEY_INFO) >= 0) {
      keyInfo = ozoneManagerClient.getKeyInfo(keyArgs, false)
          .getKeyInfo();
    } else {
      keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    }
    return keyInfo;
  }

  @Override
  public void close() throws IOException {
    if (ecReconstructExecutor.isInitialized()) {
      ecReconstructExecutor.get().shutdownNow();
    }
    if (writeExecutor.isInitialized()) {
      writeExecutor.get().shutdownNow();
    }
    IOUtils.cleanupWithLogger(LOG, ozoneManagerClient, xceiverClientManager);
    keyProviderCache.invalidateAll();
    keyProviderCache.cleanUp();
    ContainerClientMetrics.release();
  }

  @Deprecated
  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
      String bucketName, String keyName, ReplicationType type,
      ReplicationFactor factor) throws IOException {
    return initiateMultipartUpload(volumeName, bucketName, keyName,
        ReplicationConfig.fromTypeAndFactor(type, factor));
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
      String bucketName,
      String keyName,
      ReplicationConfig replicationConfig)
      throws IOException {
    return initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig,
        Collections.emptyMap());
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
      String bucketName,
      String keyName,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException {
    return initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig,
        metadata, Collections.emptyMap());
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
      String bucketName,
      String keyName,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata,
      Map<String, String> tags)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName);
    String ownerName = getRealUserInfo().getShortUserName();
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (replicationConfig != null && replicationConfig.getReplicationType()
          == HddsProtos.ReplicationType.EC) {
        throw new IOException("Can not set the replication of the file to"
            + " Erasure Coded replication, as OzoneManager does not support"
            + " Erasure Coded replication.");
      }
    }

    if (omVersion.compareTo(OzoneManagerVersion.OBJECT_TAG) < 0) {
      if (tags != null && !tags.isEmpty()) {
        throw new IOException("OzoneManager does not support object tags");
      }
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(replicationConfig)
        .addAllMetadataGdpr(metadata)
        .setOwnerName(ownerName)
        .addAllTags(tags)
        .build();
    OmMultipartInfo multipartInfo = ozoneManagerClient
        .initiateMultipartUpload(keyArgs);
    return multipartInfo;
  }

  private OpenKeySession newMultipartOpenKey(
      String volumeName, String bucketName, String keyName,
      long size, int partNumber, String uploadID,
      boolean sortDatanodesInPipeline) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName, uploadID);
    if (partNumber <= 0 || partNumber > MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD) {
      throw new OMException("Part number must be an integer between 1 and "
          + MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD + ", inclusive",
          OMException.ResultCodes.INVALID_PART);
    }
    Preconditions.checkArgument(size >= 0, "size should be greater than or " +
        "equal to zero");
    String ownerName = getRealUserInfo().getShortUserName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setIsMultipartKey(true)
        .setMultipartUploadID(uploadID)
        .setMultipartUploadPartNumber(partNumber)
        .setSortDatanodesInPipeline(sortDatanodesInPipeline)
        .setOwnerName(ownerName)
        .build();
    return ozoneManagerClient.openKey(keyArgs);
  }

  @Override
  public OzoneOutputStream createMultipartKey(
      String volumeName, String bucketName, String keyName,
      long size, int partNumber, String uploadID) throws IOException {
    final OpenKeySession openKey = newMultipartOpenKey(
        volumeName, bucketName, keyName, size, partNumber, uploadID, false);
    return createMultipartOutputStream(openKey, uploadID, partNumber);
  }

  private OzoneOutputStream createMultipartOutputStream(
      OpenKeySession openKey, String uploadID, int partNumber
  ) throws IOException {
    KeyOutputStream keyOutputStream = createKeyOutputStream(openKey)
        .setMultipartNumber(partNumber)
        .setMultipartUploadID(uploadID)
        .setIsMultipartKey(true)
        .build();
    return createOutputStream(openKey, keyOutputStream);
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(
      String volumeName,
      String bucketName,
      String keyName,
      long size,
      int partNumber,
      String uploadID)
      throws IOException {
    final OpenKeySession openKey = newMultipartOpenKey(
        volumeName, bucketName, keyName, size, partNumber, uploadID, true);
    final ByteBufferStreamOutput out;
    ReplicationConfig replicationConfig = openKey.getKeyInfo().getReplicationConfig();
    if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.RATIS) {
      KeyDataStreamOutput keyOutputStream = newKeyOutputStreamBuilder()
          .setHandler(openKey)
          .setReplicationConfig(replicationConfig)
          .setMultipartNumber(partNumber)
          .setMultipartUploadID(uploadID)
          .setIsMultipartKey(true)
          .build();
      keyOutputStream.addPreallocateBlocks(
          openKey.getKeyInfo().getLatestVersionLocations(),
          openKey.getOpenVersion());
      final OzoneOutputStream secureOut = createSecureOutputStream(openKey, keyOutputStream, null);
      out = secureOut != null ? secureOut : keyOutputStream;
    } else {
      out = createMultipartOutputStream(openKey, uploadID, partNumber);
    }
    return new OzoneDataStreamOutput(out, out);
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      String volumeName, String bucketName, String keyName, String uploadID,
      Map<Integer, String> partsMap) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName, uploadID);
    String ownerName = getRealUserInfo().getShortUserName();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setMultipartUploadID(uploadID)
        .setOwnerName(ownerName)
        .build();

    OmMultipartUploadCompleteList
        omMultipartUploadCompleteList = new OmMultipartUploadCompleteList(
        partsMap);

    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneManagerClient.completeMultipartUpload(keyArgs,
            omMultipartUploadCompleteList);

    return omMultipartUploadCompleteInfo;

  }

  @Override
  public void abortMultipartUpload(String volumeName,
       String bucketName, String keyName, String uploadID) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName, uploadID);
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setMultipartUploadID(uploadID)
        .build();
    ozoneManagerClient.abortMultipartUpload(omKeyArgs);
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(uploadID);
    Preconditions.checkArgument(maxParts > 0, "Max Parts Should be greater " +
        "than zero");
    Preconditions.checkArgument(partNumberMarker >= 0, "Part Number Marker " +
        "Should be greater than or equal to zero, as part numbers starts from" +
        " 1 and ranges till 10000");
    OmMultipartUploadListParts omMultipartUploadListParts =
        ozoneManagerClient.listParts(volumeName, bucketName, keyName,
            uploadID, partNumberMarker, maxParts);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        new OzoneMultipartUploadPartListParts(
            omMultipartUploadListParts.getReplicationConfig(),
            omMultipartUploadListParts.getNextPartNumberMarker(),
            omMultipartUploadListParts.isTruncated());

    for (OmPartInfo omPartInfo : omMultipartUploadListParts.getPartInfoList()) {
      ozoneMultipartUploadPartListParts.addPart(
          new OzoneMultipartUploadPartListParts.PartInfo(
              omPartInfo.getPartNumber(), omPartInfo.getPartName(),
              omPartInfo.getModificationTime(), omPartInfo.getSize(),
              omPartInfo.getETag()));
    }
    return ozoneMultipartUploadPartListParts;

  }

  @Override
  public OzoneMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix, String keyMarker, String uploadIdMarker, int maxUploads) throws IOException {

    OmMultipartUploadList omMultipartUploadList;
    if (omVersion.compareTo(OzoneManagerVersion.S3_LIST_MULTIPART_UPLOADS_PAGINATION) >= 0) {
      omMultipartUploadList = ozoneManagerClient.listMultipartUploads(volumeName, bucketName, prefix, keyMarker,
          uploadIdMarker, maxUploads, true);
    } else {
      omMultipartUploadList = ozoneManagerClient.listMultipartUploads(volumeName, bucketName, prefix, keyMarker,
          uploadIdMarker, maxUploads, false);
    }
    List<OzoneMultipartUpload> uploads = omMultipartUploadList.getUploads()
        .stream()
        .map(upload -> new OzoneMultipartUpload(upload.getVolumeName(),
            upload.getBucketName(),
            upload.getKeyName(),
            upload.getUploadId(),
            upload.getCreationTime(),
            upload.getReplicationConfig()))
        .collect(Collectors.toList());
    OzoneMultipartUploadList result = new OzoneMultipartUploadList(uploads,
        omMultipartUploadList.getNextKeyMarker(),
        omMultipartUploadList.getNextUploadIdMarker(),
        omMultipartUploadList.isTruncated());
    return result;
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName,
      String bucketName, String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    return ozoneManagerClient.getFileStatus(keyArgs);
  }

  @Override
  public void createDirectory(String volumeName, String bucketName,
      String keyName) throws IOException {
    String ownerName = getRealUserInfo().getShortUserName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOwnerName(ownerName)
        .build();
    ozoneManagerClient.createDirectory(keyArgs);
  }

  @Override
  public OzoneInputStream readFile(String volumeName, String bucketName,
      String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    final OmKeyInfo keyInfo;
    if (omVersion.compareTo(OzoneManagerVersion.OPTIMIZED_GET_KEY_INFO) >= 0) {
      keyInfo = ozoneManagerClient.getKeyInfo(keyArgs, false)
          .getKeyInfo();
      if (!keyInfo.isFile()) {
        throw new OMException(keyName + " is not a file.",
            OMException.ResultCodes.NOT_A_FILE);
      }
    } else {
      keyInfo = ozoneManagerClient.lookupFile(keyArgs);
    }
    return getInputStreamWithRetryFunction(keyInfo);
  }

  @Override
  @Deprecated
  public OzoneOutputStream createFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationType type, ReplicationFactor factor,
      boolean overWrite, boolean recursive) throws IOException {
    return createFile(volumeName, bucketName, keyName, size,
        ReplicationConfig.fromTypeAndFactor(type, factor), overWrite,
        recursive);
  }

  /**
   * Create InputStream with Retry function to refresh pipeline information
   * if reads fail.
   *
   * @param keyInfo
   * @return
   * @throws IOException
   */
  private OzoneInputStream getInputStreamWithRetryFunction(
      OmKeyInfo keyInfo) throws IOException {
    return createInputStream(keyInfo, omKeyInfo -> {
      try {
        return getKeyInfo(omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
            omKeyInfo.getKeyName(), true);
      } catch (IOException e) {
        LOG.error("Unable to lookup key {} on retry.", keyInfo.getKeyName(), e);
        return null;
      }
    });
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      boolean overWrite, boolean recursive) throws IOException {
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (replicationConfig.getReplicationType()
          == HddsProtos.ReplicationType.EC) {
        throw new IOException("Can not set the replication of the file to"
            + " Erasure Coded replication, as OzoneManager does not support"
            + " Erasure Coded replication.");
      }
    }
    String ownerName = getRealUserInfo().getShortUserName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setOwnerName(ownerName)
        .build();
    OpenKeySession keySession =
        ozoneManagerClient.createFile(keyArgs, overWrite, recursive);
    return createOutputStream(keySession);
  }

  private OmKeyArgs prepareOmKeyArgs(String volumeName, String bucketName,
      String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
  }

  @Override
  public OzoneDataStreamOutput createStreamFile(String volumeName,
      String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite, boolean recursive)
      throws IOException {
    String ownerName = getRealUserInfo().getShortUserName();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .setLatestVersionLocation(getLatestVersionLocation)
        .setSortDatanodesInPipeline(true)
        .setOwnerName(ownerName)
        .build();
    OpenKeySession keySession =
        ozoneManagerClient.createFile(keyArgs, overWrite, recursive);
    return createDataStreamOutput(keySession);
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey, long numEntries)
      throws IOException {
    OmKeyArgs keyArgs = prepareOmKeyArgs(volumeName, bucketName, keyName);
    return ozoneManagerClient
        .listStatus(keyArgs, recursive, startKey, numEntries);
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) throws IOException {
    OmKeyArgs keyArgs = prepareOmKeyArgs(volumeName, bucketName, keyName);
    return ozoneManagerClient
        .listStatus(keyArgs, recursive, startKey, numEntries,
            allowPartialPrefixes);
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(String volumeName,
      String bucketName, String keyName, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) throws IOException {
    OmKeyArgs keyArgs = prepareOmKeyArgs(volumeName, bucketName, keyName);
    if (omVersion.compareTo(OzoneManagerVersion.LIGHTWEIGHT_LIST_STATUS) >= 0) {
      return ozoneManagerClient.listStatusLight(keyArgs, recursive, startKey,
          numEntries, allowPartialPrefixes);
    } else {
      return ozoneManagerClient.listStatus(keyArgs, recursive, startKey,
              numEntries, allowPartialPrefixes)
          .stream()
          .map(OzoneFileStatusLight::fromOzoneFileStatus)
          .collect(Collectors.toList());
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
    return ozoneManagerClient.addAcl(obj, acl);
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
    return ozoneManagerClient.removeAcl(obj, acl);
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    return ozoneManagerClient.setAcl(obj, acls);
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return ozoneManagerClient.getAcl(obj);
  }

  static GDPRSymmetricKey getGDPRSymmetricKey(Map<String, String> metadata,
      int mode) throws Exception {
    if (!Boolean.parseBoolean(metadata.get(OzoneConsts.GDPR_FLAG))) {
      return null;
    }

    final GDPRSymmetricKey gk = new GDPRSymmetricKey(
        metadata.get(OzoneConsts.GDPR_SECRET),
        metadata.get(OzoneConsts.GDPR_ALGORITHM));
    try {
      gk.getCipher().init(mode, gk.getSecretKey());
    } catch (InvalidKeyException e) {
      if (e.getMessage().contains("Illegal key size or default parameters")) {
        LOG.error("Missing Unlimited Strength Policy jars. Please install "
            + "Java Cryptography Extension (JCE) Unlimited Strength "
            + "Jurisdiction Policy Files");
      }
      throw e;
    }
    return gk;
  }

  private OzoneInputStream createInputStream(
      OmKeyInfo keyInfo, Function<OmKeyInfo, OmKeyInfo> retryFunction)
      throws IOException {
    // When Key is not MPU or when Key is MPU and encryption is not enabled
    // Need to revisit for GDP.
    FileEncryptionInfo feInfo = keyInfo.getFileEncryptionInfo();

    if (feInfo == null) {
      LengthInputStream lengthInputStream = KeyInputStream
          .getFromOmKeyInfo(keyInfo, xceiverClientManager, retryFunction,
              blockInputStreamFactory, clientConfig);
      try {
        final GDPRSymmetricKey gk = getGDPRSymmetricKey(
            keyInfo.getMetadata(), Cipher.DECRYPT_MODE);
        if (gk != null) {
          return new OzoneInputStream(
              new CipherInputStream(lengthInputStream, gk.getCipher()));
        }
      } catch (Exception ex) {
        throw new IOException(ex);
      }
      return new OzoneInputStream(lengthInputStream.getWrappedStream());
    } else if (!keyInfo.getLatestVersionLocations().isMultipartKey()) {
      // Regular Key with FileEncryptionInfo
      LengthInputStream lengthInputStream = KeyInputStream
          .getFromOmKeyInfo(keyInfo, xceiverClientManager, retryFunction,
              blockInputStreamFactory, clientConfig);
      final KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoInputStream cryptoIn =
          new CryptoInputStream(lengthInputStream.getWrappedStream(),
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneInputStream(cryptoIn);
    } else {
      // Multipart Key with FileEncryptionInfo
      List<LengthInputStream> lengthInputStreams = KeyInputStream
          .getStreamsFromKeyInfo(keyInfo, xceiverClientManager, retryFunction,
              blockInputStreamFactory, clientConfig);
      final KeyProvider.KeyVersion decrypted = getDEK(feInfo);

      List<OzoneCryptoInputStream> cryptoInputStreams = new ArrayList<>();
      for (int i = 0; i < lengthInputStreams.size(); i++) {
        LengthInputStream lengthInputStream = lengthInputStreams.get(i);
        final OzoneCryptoInputStream ozoneCryptoInputStream =
            new OzoneCryptoInputStream(lengthInputStream,
                OzoneKMSUtil.getCryptoCodec(conf, feInfo),
                decrypted.getMaterial(), feInfo.getIV(),
                keyInfo.getKeyName(), i);
        cryptoInputStreams.add(ozoneCryptoInputStream);
      }
      return new OzoneInputStream(
          new MultipartInputStream(keyInfo.getKeyName(), cryptoInputStreams));
    }
  }

  private OzoneDataStreamOutput createDataStreamOutput(OpenKeySession openKey)
      throws IOException {
    final ReplicationConfig replicationConfig
        = openKey.getKeyInfo().getReplicationConfig();
    final ByteBufferStreamOutput out;
    if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.RATIS) {
      KeyDataStreamOutput keyOutputStream = newKeyOutputStreamBuilder()
          .setHandler(openKey)
          .setReplicationConfig(replicationConfig)
          .build();
      keyOutputStream.addPreallocateBlocks(
          openKey.getKeyInfo().getLatestVersionLocations(),
          openKey.getOpenVersion());
      final OzoneOutputStream secureOut = createSecureOutputStream(openKey, keyOutputStream, null);
      out = secureOut != null ? secureOut : keyOutputStream;
    } else {
      out = createOutputStream(openKey);
    }
    return new OzoneDataStreamOutput(out, out);
  }

  private KeyDataStreamOutput.Builder newKeyOutputStreamBuilder() {
    // Amazon S3 never adds partial objects, So for S3 requests we need to
    // set atomicKeyCreation to true
    // refer: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
    return new KeyDataStreamOutput.Builder()
        .setXceiverClientManager(xceiverClientManager)
        .setOmClient(ozoneManagerClient)
        .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
        .setConfig(clientConfig)
        .setAtomicKeyCreation(isS3GRequest.get());
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey)
      throws IOException {
    KeyOutputStream keyOutputStream = createKeyOutputStream(openKey)
        .build();
    return createOutputStream(openKey, keyOutputStream);
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey,
      KeyOutputStream keyOutputStream)
      throws IOException {
    boolean enableHsync = OzoneFSUtils.canEnableHsync(conf, true);
    keyOutputStream
        .addPreallocateBlocks(openKey.getKeyInfo().getLatestVersionLocations(),
            openKey.getOpenVersion());
    final OzoneOutputStream out = createSecureOutputStream(
        openKey, keyOutputStream, keyOutputStream);
    return out != null ? out : new OzoneOutputStream(
        keyOutputStream, enableHsync);
  }

  private OzoneOutputStream createSecureOutputStream(OpenKeySession openKey,
      OutputStream keyOutputStream, Syncable syncable) throws IOException {
    boolean enableHsync = OzoneFSUtils.canEnableHsync(conf, true);
    final FileEncryptionInfo feInfo =
        openKey.getKeyInfo().getFileEncryptionInfo();
    if (feInfo != null) {
      KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoOutputStream cryptoOut =
          new CryptoOutputStream(keyOutputStream,
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneOutputStream(cryptoOut, enableHsync);
    } else {
      try {
        final GDPRSymmetricKey gk = getGDPRSymmetricKey(
            openKey.getKeyInfo().getMetadata(), Cipher.ENCRYPT_MODE);
        if (gk != null) {
          return new OzoneOutputStream(new CipherOutputStreamOzone(
              keyOutputStream, gk.getCipher()), syncable, enableHsync);
        }
      }  catch (Exception ex) {
        throw new IOException(ex);
      }
      return null;
    }
  }

  private KeyOutputStream.Builder createKeyOutputStream(
      OpenKeySession openKey) {
    KeyOutputStream.Builder builder;

    ReplicationConfig replicationConfig =
        openKey.getKeyInfo().getReplicationConfig();
    StreamBufferArgs streamBufferArgs = StreamBufferArgs.getDefaultStreamBufferArgs(
        replicationConfig, clientConfig);
    if (replicationConfig.getReplicationType() ==
        HddsProtos.ReplicationType.EC) {
      builder = new ECKeyOutputStream.Builder()
          .setReplicationConfig((ECReplicationConfig) replicationConfig)
          .setByteBufferPool(byteBufferPool)
          .setS3CredentialsProvider(getS3CredentialsProvider());
    } else {
      builder = new KeyOutputStream.Builder()
        .setReplicationConfig(replicationConfig);
    }

    return builder.setHandler(openKey)
        .setXceiverClientManager(xceiverClientManager)
        .setOmClient(ozoneManagerClient)
        .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
        .setConfig(clientConfig)
        .setAtomicKeyCreation(isS3GRequest.get())
        .setClientMetrics(clientMetrics)
        .setExecutorServiceSupplier(writeExecutor)
        .setStreamBufferArgs(streamBufferArgs)
        .setOmVersion(omVersion);
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    URI kmsUri = getKeyProviderUri();
    if (kmsUri == null) {
      return null;
    }

    try {
      return keyProviderCache.get(kmsUri, new Callable<KeyProvider>() {
        @Override
        public KeyProvider call() throws Exception {
          return OzoneKMSUtil.getKeyProvider(conf, kmsUri);
        }
      });
    } catch (Exception e) {
      LOG.error("Can't create KeyProvider for Ozone RpcClient.", e);
      return null;
    }
  }

  @Override
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    long now = Time.monotonicNow();
    if ((serverDefaults == null) ||
        (now - serverDefaultsLastUpdate > serverDefaultsValidityPeriod)) {
      try {
        for (ServiceInfo si : ozoneManagerClient.getServiceInfo()
            .getServiceInfoList()) {
          if (si.getServerDefaults() != null) {
            serverDefaults = si.getServerDefaults();
            serverDefaultsLastUpdate = now;
            break;
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not get server defaults from OM.", e);
      }
    }
    return serverDefaults;
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    String keyProviderUri = (getServerDefaults() != null) ?
        serverDefaults.getKeyProviderUri() : null;
    return OzoneKMSUtil.getKeyProviderUri(ugi, null, keyProviderUri, conf);
  }

  @Override
  public String getCanonicalServiceName() {
    return (dtService != null) ? dtService.toString() : null;
  }

  @Override
  @VisibleForTesting
  public OzoneManagerProtocol getOzoneManagerClient() {
    return ozoneManagerClient;
  }

  @VisibleForTesting
  public Cache<URI, KeyProvider> getKeyProviderCache() {
    return keyProviderCache;
  }

  @Override
  public OzoneKey headObject(String volumeName, String bucketName,
      String keyName) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setLatestVersionLocation(true)
        .setHeadOp(true)
        .setForceUpdateContainerCacheFromSCM(false)
        .build();
    OmKeyInfo keyInfo = getKeyInfo(keyArgs);

    return OzoneKey.fromKeyInfo(keyInfo);
  }

  @Override
  public OzoneKey headS3Object(String bucketName, String keyName)
      throws IOException {
    OmKeyInfo keyInfo = getS3KeyInfo(bucketName, keyName, true);
    return OzoneKey.fromKeyInfo(keyInfo);
  }

  @Override
  public void setThreadLocalS3Auth(
      S3Auth ozoneSharedSecretAuth) {
    ozoneManagerClient.setThreadLocalS3Auth(ozoneSharedSecretAuth);
    this.s3gUgi = UserGroupInformation.createRemoteUser(getThreadLocalS3Auth().getUserPrincipal());
  }

  @Override
  public void setIsS3Request(boolean s3Request) {
    this.isS3GRequest.set(s3Request);
  }

  @Override
  public S3Auth getThreadLocalS3Auth() {
    return ozoneManagerClient.getThreadLocalS3Auth();
  }

  @Override
  public void clearThreadLocalS3Auth() {
    ozoneManagerClient.clearThreadLocalS3Auth();
  }

  @Override
  public ThreadLocal<S3Auth> getS3CredentialsProvider() {
    return ozoneManagerClient.getS3CredentialsProvider();
  }

  @Override
  public boolean setBucketOwner(String volumeName, String bucketName,
      String owner) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(owner);
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setOwnerName(owner);
    return ozoneManagerClient.setBucketOwner(builder.build());
  }

  @Override
  public void setTimes(OzoneObj obj, String keyName, long mtime, long atime)
      throws IOException {
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(obj.getVolumeName())
        .setBucketName(obj.getBucketName())
        .setKeyName(keyName);
    ozoneManagerClient.setTimes(builder.build(), mtime, atime);
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName,
                                   String keyName, boolean force)
      throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.HBASE_SUPPORT) < 0) {
      throw new UnsupportedOperationException("Lease recovery API requires OM version "
          + OzoneManagerVersion.HBASE_SUPPORT + " or later. Current OM version "
          + omVersion);
    }
    return ozoneManagerClient.recoverLease(volumeName, bucketName, keyName, force);
  }

  @Override
  public void recoverKey(OmKeyArgs args, long clientID) throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.HBASE_SUPPORT) < 0) {
      throw new UnsupportedOperationException("Lease recovery API requires OM version "
          + OzoneManagerVersion.HBASE_SUPPORT + " or later. Current OM version "
          + omVersion);
    }
    ozoneManagerClient.recoverKey(args, clientID);
  }

  @Override
  public Map<String, String> getObjectTagging(String volumeName, String bucketName, String keyName)
      throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.S3_OBJECT_TAGGING_API) < 0) {
      throw new IOException("OzoneManager does not support S3 object tagging API");
    }

    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    return ozoneManagerClient.getObjectTagging(keyArgs);
  }

  @Override
  public void putObjectTagging(String volumeName, String bucketName,
                               String keyName, Map<String, String> tags) throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.S3_OBJECT_TAGGING_API) < 0) {
      throw new IOException("OzoneManager does not support S3 object tagging API");
    }

    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .addAllTags(tags)
        .build();
    ozoneManagerClient.putObjectTagging(keyArgs);
  }

  @Override
  public void deleteObjectTagging(String volumeName, String bucketName,
                                  String keyName) throws IOException {
    if (omVersion.compareTo(OzoneManagerVersion.S3_OBJECT_TAGGING_API) < 0) {
      throw new IOException("OzoneManager does not support S3 object tagging API");
    }

    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    ozoneManagerClient.deleteObjectTagging(keyArgs);
  }

  @Override
  public AssumeRoleResponseInfo assumeRole(String roleArn, String roleSessionName, int durationSeconds,
      String awsIamSessionPolicy) throws IOException {
    return ozoneManagerClient.assumeRole(roleArn, roleSessionName, durationSeconds, awsIamSessionPolicy);
  }

  @Override
  public void revokeSTSToken(String sessionToken) throws IOException {
    ozoneManagerClient.revokeSTSToken(sessionToken);
  }

  private static ExecutorService createThreadPoolExecutor(
       int corePoolSize, int maximumPoolSize, String threadNameFormat) {
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
            60, TimeUnit.SECONDS, new SynchronousQueue<>(),
               new ThreadFactoryBuilder().setNameFormat(threadNameFormat).setDaemon(true).build(),
               new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
