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

package org.apache.hadoop.ozone.client.rpc;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.function.SupplierWithIOException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
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
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.TenantArgs;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.io.MultipartCryptoKeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneCryptoInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
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
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
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
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_MAXIMUM_ACCESS_ID_LENGTH;

import org.apache.logging.log4j.util.Strings;
import org.apache.ratis.protocol.ClientId;
import org.jetbrains.annotations.NotNull;
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

  private final ConfigurationSource conf;
  private final OzoneManagerClientProtocol ozoneManagerClient;
  private final XceiverClientFactory xceiverClientManager;
  private final int chunkSize;
  private final UserGroupInformation ugi;
  private final ACLType userRights;
  private final ACLType groupRights;
  private final long blockSize;
  private final ClientId clientId = ClientId.randomId();
  private final boolean unsafeByteBufferConversion;
  private Text dtService;
  private final boolean topologyAwareReadEnabled;
  private final boolean checkKeyNameEnabled;
  private final OzoneClientConfig clientConfig;
  private final Cache<URI, KeyProvider> keyProviderCache;
  private final boolean getLatestVersionLocation;
  private final ByteBufferPool byteBufferPool;
  private final BlockInputStreamFactory blockInputStreamFactory;
  private final OzoneManagerVersion omVersion;
  private volatile ExecutorService ecReconstructExecutor;
  private final ContainerClientMetrics clientMetrics;

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
    // Get default acl rights for user and group.
    OzoneAclConfig aclConfig = this.conf.getObject(OzoneAclConfig.class);
    this.userRights = aclConfig.getUserDefaultRights();
    this.groupRights = aclConfig.getGroupDefaultRights();

    this.clientConfig = conf.getObject(OzoneClientConfig.class);

    OmTransport omTransport = createOmTransport(omServiceId);
    OzoneManagerProtocolClientSideTranslatorPB
        ozoneManagerProtocolClientSideTranslatorPB =
        new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
        clientId.toString());
    this.ozoneManagerClient = TracingUtil.createProxy(
        ozoneManagerProtocolClientSideTranslatorPB,
        OzoneManagerClientProtocol.class, conf);
    dtService = omTransport.getDelegationTokenService();
    List<X509Certificate> x509Certificates = null;
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
      String caCertPem = null;
      List<String> caCertPems = null;
      caCertPem = serviceInfoEx.getCaCertificate();
      caCertPems = serviceInfoEx.getCaCertPemList();
      if (caCertPems == null || caCertPems.isEmpty()) {
        if (caCertPem == null) {
          LOG.error("RpcClient received empty caCertPems from serviceInfo");
          CertificateException ex = new CertificateException(
              "No caCerts found; caCertPem can" +
                  " not be null when caCertPems is empty or null"
          );
          throw new IOException(ex);
        }
        caCertPems = Collections.singletonList(caCertPem);
      }
      x509Certificates = OzoneSecurityUtil.convertToX509(caCertPems);
    }

    this.xceiverClientManager =
        createXceiverClientFactory(x509Certificates);

    int configuredChunkSize = (int) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
            ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    if (configuredChunkSize > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
              + " the maximum size ({}),"
              + " resetting to the maximum size.",
          configuredChunkSize, OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
    } else {
      chunkSize = configuredChunkSize;
    }

    blockSize = (long) conf.getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    unsafeByteBufferConversion = conf.getBoolean(
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);

    topologyAwareReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    checkKeyNameEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
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
    this.byteBufferPool = new ElasticByteBufferPool();
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, this::getECReconstructExecutor);
    this.clientMetrics = ContainerClientMetrics.acquire();
  }

  public XceiverClientFactory getXceiverClientManager() {
    return xceiverClientManager;
  }

  private OzoneManagerVersion getOmVersion(ServiceInfoEx info) {
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

  @NotNull
  @VisibleForTesting
  protected XceiverClientFactory createXceiverClientFactory(
      List<X509Certificate> x509Certificates) throws IOException {
    return new XceiverClientManager(conf,
        conf.getObject(XceiverClientManager.ScmClientConfig.class),
        x509Certificates);
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
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    //User ACL
    listOfAcls.add(new OzoneAcl(ACLIdentityType.USER,
        owner, userRights, ACCESS));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(owner).getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(ACLIdentityType.GROUP, group, groupRights, ACCESS)));
    //ACLs from VolumeArgs
    if (volArgs.getAcls() != null) {
      listOfAcls.addAll(volArgs.getAcls());
    }

    OmVolumeArgs.Builder builder = OmVolumeArgs.newBuilder();
    builder.setVolume(volumeName);
    builder.setAdminName(admin);
    builder.setOwnerName(owner);
    builder.setQuotaInBytes(quotaInBytes);
    builder.setQuotaInNamespace(quotaInNamespace);
    builder.setUsedNamespace(0L);
    builder.addAllMetadata(volArgs.getMetadata());

    //Remove duplicates and add ACLs
    for (OzoneAcl ozoneAcl :
        listOfAcls.stream().distinct().collect(Collectors.toList())) {
      builder.addOzoneAcls(ozoneAcl);
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
    HddsClientUtils.verifyResourceName(volumeName);
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
    return ozoneManagerClient.getS3VolumeContext();
  }

  public OzoneVolume buildOzoneVolume(OmVolumeArgs volume) {
    return new OzoneVolume(
        conf,
        this,
        volume.getVolume(),
        volume.getAdminName(),
        volume.getOwnerName(),
        volume.getQuotaInBytes(),
        volume.getQuotaInNamespace(),
        volume.getUsedNamespace(),
        volume.getCreationTime(),
        volume.getModificationTime(),
        volume.getAcls(),
        volume.getMetadata(),
        volume.getRefCount());
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

    return volumes.stream().map(volume -> new OzoneVolume(
        conf,
        this,
        volume.getVolume(),
        volume.getAdminName(),
        volume.getOwnerName(),
        volume.getQuotaInBytes(),
        volume.getQuotaInNamespace(),
        volume.getUsedNamespace(),
        volume.getCreationTime(),
        volume.getModificationTime(),
        volume.getAcls()))
        .collect(Collectors.toList());
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                       String prevVolume, int maxListResult)
      throws IOException {
    List<OmVolumeArgs> volumes = ozoneManagerClient.listVolumeByUser(
        user, volumePrefix, prevVolume, maxListResult);

    return volumes.stream().map(volume -> new OzoneVolume(
        conf,
        this,
        volume.getVolume(),
        volume.getAdminName(),
        volume.getOwnerName(),
        volume.getQuotaInBytes(),
        volume.getQuotaInNamespace(),
        volume.getUsedNamespace(),
        volume.getCreationTime(),
        volume.getModificationTime(),
        volume.getAcls(),
        volume.getMetadata()))
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

    Boolean isVersionEnabled = bucketArgs.getVersioning() == null ?
        Boolean.FALSE : bucketArgs.getVersioning();
    StorageType storageType = bucketArgs.getStorageType() == null ?
        StorageType.DEFAULT : bucketArgs.getStorageType();
    BucketLayout bucketLayout = bucketArgs.getBucketLayout();
    BucketEncryptionKeyInfo bek = null;
    if (bucketArgs.getEncryptionKey() != null) {
      bek = new BucketEncryptionKeyInfo.Builder()
          .setKeyName(bucketArgs.getEncryptionKey()).build();
    }

    List<OzoneAcl> listOfAcls = getAclList();
    //ACLs from BucketArgs
    if (bucketArgs.getAcls() != null) {
      listOfAcls.addAll(bucketArgs.getAcls());
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
        .setAcls(listOfAcls.stream().distinct().collect(Collectors.toList()))
        .setBucketLayout(bucketLayout)
        .setOwner(owner);

    if (bek != null) {
      builder.setBucketEncryptionKey(bek);
    }

    DefaultReplicationConfig defaultReplicationConfig =
        bucketArgs.getDefaultReplicationConfig();
    if (defaultReplicationConfig != null) {
      builder.setDefaultReplicationConfig(defaultReplicationConfig);
    }

    String layoutMsg = bucketLayout != null
        ? "with bucket layout " + bucketLayout
        : "with server-side default bucket layout";
    LOG.info("Creating Bucket: {}/{}, {}, {} as owner, Versioning {}, " +
            "Storage Type set to {} and Encryption set to {} ",
        volumeName, bucketName, layoutMsg, owner, isVersionEnabled,
        storageType, bek != null);
    ozoneManagerClient.createBucket(builder.build());
  }

  private static void verifyVolumeName(String volumeName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(volumeName);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }
  }

  private static void verifyBucketName(String bucketName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(bucketName);
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
   * Helper function to get default acl list for current user.
   *
   * @return listOfAcls
   * */
  private List<OzoneAcl> getAclList() {
    if (ozoneManagerClient.getThreadLocalS3Auth() != null) {
      UserGroupInformation aclUgi =
          UserGroupInformation.createRemoteUser(
             ozoneManagerClient.getThreadLocalS3Auth().getAccessID());
      return OzoneAclUtil.getAclList(
          aclUgi.getUserName(),
          aclUgi.getGroupNames(),
         userRights, groupRights);
    }
    return OzoneAclUtil.getAclList(ugi.getUserName(), ugi.getGroupNames(),
        userRights, groupRights);
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
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(kerberosID),
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
    Preconditions.checkArgument(Strings.isNotBlank(kerberosID),
            "kerberosID cannot be null or empty.");
    // No need to check createIfNotExist here which is a primitive
    return ozoneManagerClient.getS3Secret(kerberosID, createIfNotExist);
  }

  /**
   * {@inheritDoc}
   */
  public S3SecretValue setS3Secret(String accessId, String secretKey)
          throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(accessId),
            "accessId cannot be null or empty.");
    Preconditions.checkArgument(Strings.isNotBlank(secretKey),
            "secretKey cannot be null or empty.");
    return ozoneManagerClient.setS3Secret(accessId, secretKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void revokeS3Secret(String kerberosID) throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(kerberosID),
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
    Preconditions.checkArgument(Strings.isNotBlank(tenantId),
        "tenantId cannot be null or empty.");
    Preconditions.checkNotNull(tenantArgs);

    final String volumeName = tenantArgs.getVolumeName();
    verifyVolumeName(volumeName);

    OmTenantArgs.Builder builder = OmTenantArgs.newBuilder();
    builder.setTenantId(tenantId);
    builder.setVolumeName(volumeName);
    // TODO: Add more fields
    // TODO: Include OmVolumeArgs in (Om)TenantArgs as well for volume creation?

    LOG.info("Creating Tenant: '{}', with new volume: '{}'",
        tenantId, volumeName);

    ozoneManagerClient.createTenant(builder.build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DeleteTenantState deleteTenant(String tenantId) throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(tenantId),
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
    Preconditions.checkArgument(Strings.isNotBlank(username),
        "username can't be null or empty.");
    Preconditions.checkArgument(Strings.isNotBlank(tenantId),
        "tenantId can't be null or empty.");
    Preconditions.checkArgument(Strings.isNotBlank(accessId),
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
    Preconditions.checkArgument(Strings.isNotBlank(accessId),
        "accessId can't be null or empty.");
    ozoneManagerClient.tenantRevokeUserAccessId(accessId);
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
    Preconditions.checkArgument(Strings.isNotBlank(accessId),
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
    Preconditions.checkArgument(Strings.isNotBlank(accessId),
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
    Preconditions.checkArgument(Strings.isNotBlank(userPrincipal),
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
    HddsClientUtils.verifyResourceName(bucketName);
    HddsClientUtils.verifyResourceName(volumeName);
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
    return new OzoneBucket(
        conf,
        this,
        bucketInfo.getVolumeName(),
        bucketInfo.getBucketName(),
        bucketInfo.getStorageType(),
        bucketInfo.getIsVersionEnabled(),
        bucketInfo.getCreationTime(),
        bucketInfo.getModificationTime(),
        bucketInfo.getMetadata(),
        bucketInfo.getEncryptionKeyInfo() != null ? bucketInfo
            .getEncryptionKeyInfo().getKeyName() : null,
        bucketInfo.getSourceVolume(),
        bucketInfo.getSourceBucket(),
        bucketInfo.getUsedBytes(),
        bucketInfo.getUsedNamespace(),
        bucketInfo.getQuotaInBytes(),
        bucketInfo.getQuotaInNamespace(),
        bucketInfo.getBucketLayout(),
        bucketInfo.getOwner(),
        bucketInfo.getDefaultReplicationConfig()
    );
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                       String prevBucket, int maxListResult)
      throws IOException {
    List<OmBucketInfo> buckets = ozoneManagerClient.listBuckets(
        volumeName, prevBucket, bucketPrefix, maxListResult);

    return buckets.stream().map(bucket -> new OzoneBucket(
        conf,
        this,
        bucket.getVolumeName(),
        bucket.getBucketName(),
        bucket.getStorageType(),
        bucket.getIsVersionEnabled(),
        bucket.getCreationTime(),
        bucket.getModificationTime(),
        bucket.getMetadata(),
        bucket.getEncryptionKeyInfo() != null ? bucket
            .getEncryptionKeyInfo().getKeyName() : null,
        bucket.getSourceVolume(),
        bucket.getSourceBucket(),
        bucket.getUsedBytes(),
        bucket.getUsedNamespace(),
        bucket.getQuotaInBytes(),
        bucket.getQuotaInNamespace(),
        bucket.getBucketLayout(),
        bucket.getOwner(),
        bucket.getDefaultReplicationConfig()))
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
      ReplicationConfigValidator validator =
              this.conf.getObject(ReplicationConfigValidator.class);
      validator.validate(replicationConfig);
    }
    String requestId = UUID.randomUUID().toString();

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .addAllMetadata(metadata)
        .setAcls(getAclList())
        .setLatestVersionLocation(getLatestVersionLocation);
    if (Boolean.parseBoolean(metadata.get(OzoneConsts.GDPR_FLAG))) {
      try {
        GDPRSymmetricKey gKey = new GDPRSymmetricKey(new SecureRandom());
        builder.addAllMetadata(gKey.getKeyDetails());
      } catch (Exception e) {
        if (e instanceof InvalidKeyException &&
            e.getMessage().contains("Illegal key size or default parameters")) {
          LOG.error("Missing Unlimited Strength Policy jars. Please install " +
              "Java Cryptography Extension (JCE) Unlimited Strength " +
              "Jurisdiction Policy Files");
        }
        throw new IOException(e);
      }
    }

    OpenKeySession openKey = ozoneManagerClient.openKey(builder.build());
    return createOutputStream(openKey, requestId);
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
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    return getInputStreamWithRetryFunction(keyInfo);
  }

  @Override
  public Map< OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream> >
      getKeysEveryReplicas(String volumeName,
                         String bucketName,
                         String keyName) throws IOException {

    Map< OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream> > result
        = new LinkedHashMap<>();

    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();

    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos
        = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();

    for (OmKeyLocationInfo keyLocationInfo : keyLocationInfos) {
      Map<DatanodeDetails, OzoneInputStream> blocks = new HashMap<>();

      Pipeline pipelineBefore = keyLocationInfo.getPipeline();
      List<DatanodeDetails> datanodes = pipelineBefore.getNodes();

      for (DatanodeDetails dn : datanodes) {
        List<DatanodeDetails> nodes = new ArrayList<>();
        nodes.add(dn);
        Pipeline pipeline
            = new Pipeline.Builder(pipelineBefore).setNodes(nodes)
            .setId(PipelineID.randomId()).build();
        keyLocationInfo.setPipeline(pipeline);

        List<OmKeyLocationInfo> keyLocationInfoList = new ArrayList<>();
        keyLocationInfoList.add(keyLocationInfo);
        OmKeyLocationInfoGroup keyLocationInfoGroup
            = new OmKeyLocationInfoGroup(0, keyLocationInfoList);
        List<OmKeyLocationInfoGroup> keyLocationInfoGroups = new ArrayList<>();
        keyLocationInfoGroups.add(keyLocationInfoGroup);

        keyInfo.setKeyLocationVersions(keyLocationInfoGroups);
        OzoneInputStream is = createInputStream(keyInfo, Function.identity());

        blocks.put(dn, is);
      }

      result.put(keyLocationInfo, blocks);
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
    HddsClientUtils.verifyResourceName(volumeName, bucketName);
    Preconditions.checkNotNull(keyNameList);
    OmDeleteKeys omDeleteKeys = new OmDeleteKeys(volumeName, bucketName,
        keyNameList);
    ozoneManagerClient.deleteKeys(omDeleteKeys);
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
    List<OmKeyInfo> keys = ozoneManagerClient.listKeys(
        volumeName, bucketName, prevKey, keyPrefix, maxListResult);
    return keys.stream().map(key -> new OzoneKey(
        key.getVolumeName(),
        key.getBucketName(),
        key.getKeyName(),
        key.getDataSize(),
        key.getCreationTime(),
        key.getModificationTime(),
        key.getReplicationConfig()))
        .collect(Collectors.toList());
  }

  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException {

    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    return ozoneManagerClient.listTrash(volumeName, bucketName, startKeyName,
        keyPrefix, maxKeys);
  }

  @Override
  public boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException {

    return ozoneManagerClient.recoverTrash(volumeName, bucketName, keyName,
        destinationBucket);
  }

  @Override
  public OzoneKeyDetails getKeyDetails(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);

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

    SupplierWithIOException<OzoneInputStream> getInputStream =
        () -> getInputStreamWithRetryFunction(keyInfo);

    return new OzoneKeyDetails(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), ozoneKeyLocations,
        keyInfo.getReplicationConfig(), keyInfo.getMetadata(),
        keyInfo.getFileEncryptionInfo(),
        getInputStream);
  }

  @Override
  public void close() throws IOException {
    if (ecReconstructExecutor != null) {
      ecReconstructExecutor.shutdownNow();
      ecReconstructExecutor = null;
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
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName);
    if (omVersion
        .compareTo(OzoneManagerVersion.ERASURE_CODED_STORAGE_SUPPORT) < 0) {
      if (replicationConfig.getReplicationType()
          == HddsProtos.ReplicationType.EC) {
        throw new IOException("Can not set the replication of the file to"
            + " Erasure Coded replication, as OzoneManager does not support"
            + " Erasure Coded replication.");
      }
    }
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(replicationConfig)
        .setAcls(getAclList())
        .build();
    OmMultipartInfo multipartInfo = ozoneManagerClient
        .initiateMultipartUpload(keyArgs);
    return multipartInfo;
  }

  @Override
  public OzoneOutputStream createMultipartKey(String volumeName,
                                              String bucketName,
                                              String keyName,
                                              long size,
                                              int partNumber,
                                              String uploadID)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName, uploadID);
    Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000, "Part " +
        "number should be greater than zero and less than or equal to 10000");
    Preconditions.checkArgument(size >= 0, "size should be greater than or " +
        "equal to zero");
    String requestId = UUID.randomUUID().toString();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setIsMultipartKey(true)
        .setMultipartUploadID(uploadID)
        .setMultipartUploadPartNumber(partNumber)
        .setAcls(getAclList())
        .build();

    OpenKeySession openKey = ozoneManagerClient.openKey(keyArgs);
    KeyOutputStream keyOutputStream = createKeyOutputStream(openKey, requestId)
        .setMultipartNumber(partNumber)
        .setMultipartUploadID(uploadID)
        .setIsMultipartKey(true)
        .build();
    keyOutputStream.addPreallocateBlocks(
        openKey.getKeyInfo().getLatestVersionLocations(),
        openKey.getOpenVersion());
    FileEncryptionInfo feInfo = openKey.getKeyInfo().getFileEncryptionInfo();
    if (feInfo != null) {
      KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoOutputStream cryptoOut =
          new CryptoOutputStream(keyOutputStream,
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneOutputStream(cryptoOut);
    } else {
      return new OzoneOutputStream(keyOutputStream);
    }
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      String volumeName, String bucketName, String keyName, String uploadID,
      Map<Integer, String> partsMap) throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName, uploadID);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setMultipartUploadID(uploadID)
        .setAcls(getAclList())
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
              omPartInfo.getModificationTime(), omPartInfo.getSize()));
    }
    return ozoneMultipartUploadPartListParts;

  }

  @Override
  public OzoneMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix) throws IOException {

    OmMultipartUploadList omMultipartUploadList =
        ozoneManagerClient.listMultipartUploads(volumeName, bucketName, prefix);
    List<OzoneMultipartUpload> uploads = omMultipartUploadList.getUploads()
        .stream()
        .map(upload -> new OzoneMultipartUpload(upload.getVolumeName(),
            upload.getBucketName(),
            upload.getKeyName(),
            upload.getUploadId(),
            upload.getCreationTime(),
            upload.getReplicationConfig()))
        .collect(Collectors.toList());
    OzoneMultipartUploadList result = new OzoneMultipartUploadList(uploads);
    return result;
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName,
      String bucketName, String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    return ozoneManagerClient.getFileStatus(keyArgs);
  }

  @Override
  public void createDirectory(String volumeName, String bucketName,
      String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(getAclList())
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
    OmKeyInfo keyInfo = ozoneManagerClient.lookupFile(keyArgs);
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
        OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
            .setVolumeName(omKeyInfo.getVolumeName())
            .setBucketName(omKeyInfo.getBucketName())
            .setKeyName(omKeyInfo.getKeyName())
            .setRefreshPipeline(true)
            .setSortDatanodesInPipeline(topologyAwareReadEnabled)
            .setLatestVersionLocation(getLatestVersionLocation)
            .build();
        return ozoneManagerClient.lookupKey(omKeyArgs);
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
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .setAcls(getAclList())
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
    OpenKeySession keySession =
        ozoneManagerClient.createFile(keyArgs, overWrite, recursive);
    return createOutputStream(keySession, UUID.randomUUID().toString());
  }

  private OmKeyArgs prepareOmKeyArgs(String volumeName, String bucketName,
      String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .setLatestVersionLocation(getLatestVersionLocation)
        .build();
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

  private OzoneInputStream createInputStream(
      OmKeyInfo keyInfo, Function<OmKeyInfo, OmKeyInfo> retryFunction)
      throws IOException {
    // When Key is not MPU or when Key is MPU and encryption is not enabled
    // Need to revisit for GDP.
    FileEncryptionInfo feInfo = keyInfo.getFileEncryptionInfo();

    if (feInfo == null) {
      LengthInputStream lengthInputStream = KeyInputStream
          .getFromOmKeyInfo(keyInfo, xceiverClientManager,
              clientConfig.isChecksumVerify(), retryFunction,
              blockInputStreamFactory);
      try {
        Map< String, String > keyInfoMetadata = keyInfo.getMetadata();
        if (Boolean.valueOf(keyInfoMetadata.get(OzoneConsts.GDPR_FLAG))) {
          GDPRSymmetricKey gk = new GDPRSymmetricKey(
              keyInfoMetadata.get(OzoneConsts.GDPR_SECRET),
              keyInfoMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.DECRYPT_MODE, gk.getSecretKey());
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
          .getFromOmKeyInfo(keyInfo, xceiverClientManager,
              clientConfig.isChecksumVerify(), retryFunction,
              blockInputStreamFactory);
      final KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoInputStream cryptoIn =
          new CryptoInputStream(lengthInputStream.getWrappedStream(),
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneInputStream(cryptoIn);
    } else {
      // Multipart Key with FileEncryptionInfo
      List<LengthInputStream> lengthInputStreams = KeyInputStream
          .getStreamsFromKeyInfo(keyInfo, xceiverClientManager,
              clientConfig.isChecksumVerify(), retryFunction,
              blockInputStreamFactory);
      final KeyProvider.KeyVersion decrypted = getDEK(feInfo);

      List<OzoneCryptoInputStream> cryptoInputStreams = new ArrayList<>();
      for (LengthInputStream lengthInputStream : lengthInputStreams) {
        final OzoneCryptoInputStream ozoneCryptoInputStream =
            new OzoneCryptoInputStream(lengthInputStream,
                OzoneKMSUtil.getCryptoCodec(conf, feInfo),
                decrypted.getMaterial(), feInfo.getIV());
        cryptoInputStreams.add(ozoneCryptoInputStream);
      }
      return new MultipartCryptoKeyInputStream(keyInfo.getKeyName(),
          cryptoInputStreams);
    }
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey,
      String requestId) throws IOException {

    KeyOutputStream keyOutputStream = createKeyOutputStream(openKey, requestId)
        .build();

    keyOutputStream
        .addPreallocateBlocks(openKey.getKeyInfo().getLatestVersionLocations(),
            openKey.getOpenVersion());
    final FileEncryptionInfo feInfo =
        openKey.getKeyInfo().getFileEncryptionInfo();
    if (feInfo != null) {
      KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoOutputStream cryptoOut =
          new CryptoOutputStream(keyOutputStream,
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneOutputStream(cryptoOut);
    } else {
      try {
        GDPRSymmetricKey gk;
        Map<String, String> openKeyMetadata =
            openKey.getKeyInfo().getMetadata();
        if (Boolean.valueOf(openKeyMetadata.get(OzoneConsts.GDPR_FLAG))) {
          gk = new GDPRSymmetricKey(
              openKeyMetadata.get(OzoneConsts.GDPR_SECRET),
              openKeyMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.ENCRYPT_MODE, gk.getSecretKey());
          return new OzoneOutputStream(
              new CipherOutputStream(keyOutputStream, gk.getCipher()));
        }
      }  catch (Exception ex) {
        throw new IOException(ex);
      }

      return new OzoneOutputStream(keyOutputStream);
    }
  }

  private KeyOutputStream.Builder createKeyOutputStream(OpenKeySession openKey,
      String requestId) {
    KeyOutputStream.Builder builder;

    ReplicationConfig replicationConfig =
        openKey.getKeyInfo().getReplicationConfig();
    if (replicationConfig.getReplicationType() ==
        HddsProtos.ReplicationType.EC) {
      builder = new ECKeyOutputStream.Builder()
          .setReplicationConfig((ECReplicationConfig) replicationConfig)
          .setByteBufferPool(byteBufferPool);
    } else {
      builder = new KeyOutputStream.Builder()
        .setReplicationConfig(replicationConfig);
    }

    return builder.setHandler(openKey)
        .setXceiverClientManager(xceiverClientManager)
        .setOmClient(ozoneManagerClient)
        .setRequestID(requestId)
        .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
        .setConfig(clientConfig)
        .setClientMetrics(clientMetrics);
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
  public URI getKeyProviderUri() throws IOException {
    // TODO: fix me to support kms instances for difference OMs
    return OzoneKMSUtil.getKeyProviderUri(ugi,
        null, null, conf);
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
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);

    return new OzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), keyInfo.getReplicationConfig());

  }

  @Override
  public void setThreadLocalS3Auth(
      S3Auth ozoneSharedSecretAuth) {
    ozoneManagerClient.setThreadLocalS3Auth(ozoneSharedSecretAuth);
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

  public ExecutorService getECReconstructExecutor() {
    // local ref to a volatile to ensure access
    // to a completed initialized object
    ExecutorService executor = ecReconstructExecutor;
    if (executor == null) {
      synchronized (this) {
        executor = ecReconstructExecutor;
        if (executor == null) {
          ecReconstructExecutor = new ThreadPoolExecutor(
              EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE,
              clientConfig.getEcReconstructStripeReadPoolLimit(),
              60, TimeUnit.SECONDS, new SynchronousQueue<>(),
              new ThreadFactoryBuilder()
                  .setNameFormat("ec-reconstruct-reader-TID-%d")
                  .build(),
              new ThreadPoolExecutor.CallerRunsPolicy());
          executor = ecReconstructExecutor;
        }
      }
    }
    return executor;
  }
}
