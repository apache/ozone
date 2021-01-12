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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
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
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
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
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import org.apache.logging.log4j.util.Strings;
import org.apache.ratis.protocol.ClientId;
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

  private final ConfigurationSource conf;
  private final OzoneManagerProtocol ozoneManagerClient;
  private final XceiverClientManager xceiverClientManager;
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

    OmTransport omTransport = OmTransportFactory.create(conf, ugi, omServiceId);

    this.ozoneManagerClient = TracingUtil.createProxy(
        new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
            clientId.toString()),
        OzoneManagerProtocol.class, conf
    );
    dtService = omTransport.getDelegationTokenService();
    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    String caCertPem = null;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      caCertPem = serviceInfoEx.getCaCertificate();
    }

    this.xceiverClientManager = new XceiverClientManager(conf,
        conf.getObject(XceiverClientManager.ScmClientConfig.class), caCertPem);

    int configuredChunkSize = (int) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
            ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    if(configuredChunkSize > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
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
        ugi.getUserName() : volArgs.getAdmin();
    String owner = volArgs.getOwner() == null ?
        ugi.getUserName() : volArgs.getOwner();
    long quotaInNamespace = getQuotaValue(volArgs.getQuotaInNamespace());
    long quotaInBytes = getQuotaValue(volArgs.getQuotaInBytes());
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
      builder.addOzoneAcls(OzoneAcl.toProtobuf(ozoneAcl));
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
    ozoneManagerClient.setQuota(volumeName, quotaInNamespace, quotaInBytes);
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName)
      throws IOException {
    verifyVolumeName(volumeName);
    OmVolumeArgs volume = ozoneManagerClient.getVolumeInfo(volumeName);
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
        volume.getAclMap().ozoneAclGetProtobuf().stream().
            map(OzoneAcl::fromProtobuf).collect(Collectors.toList()),
        volume.getMetadata());
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
        volume.getAclMap().ozoneAclGetProtobuf().stream().
            map(OzoneAcl::fromProtobuf).collect(Collectors.toList())))
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
        volume.getAclMap().ozoneAclGetProtobuf().stream().
            map(OzoneAcl::fromProtobuf).collect(Collectors.toList()),
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

    Boolean isVersionEnabled = bucketArgs.getVersioning() == null ?
        Boolean.FALSE : bucketArgs.getVersioning();
    StorageType storageType = bucketArgs.getStorageType() == null ?
        StorageType.DEFAULT : bucketArgs.getStorageType();
    BucketEncryptionKeyInfo bek = null;
    if (bucketArgs.getEncryptionKey() != null) {
      bek = new BucketEncryptionKeyInfo.Builder()
          .setKeyName(bucketArgs.getEncryptionKey()).build();
    }

    List<OzoneAcl> listOfAcls = getAclList();
    //ACLs from BucketArgs
    if(bucketArgs.getAcls() != null) {
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
        .setQuotaInBytes(getQuotaValue(bucketArgs.getQuotaInBytes()))
        .setQuotaInNamespace(getQuotaValue(bucketArgs.getQuotaInNamespace()))
        .setAcls(listOfAcls.stream().distinct().collect(Collectors.toList()));

    if (bek != null) {
      builder.setBucketEncryptionKey(bek);
    }

    LOG.info("Creating Bucket: {}/{}, with Versioning {} and " +
            "Storage Type set to {} and Encryption set to {} ",
        volumeName, bucketName, isVersionEnabled, storageType, bek != null);
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
    if (quota < OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "counts quota is :" + quota + ".");
    }
  }

  private static void verifySpaceQuota(long quota) throws OMException {
    if (quota < OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "space quota is :" + quota + ".");
    }
  }

  private static long getQuotaValue(long quota) throws OMException {
    if (quota == 0) {
      return OzoneConsts.QUOTA_RESET;
    } else {
      return quota;
    }
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return listOfAcls
   * */
  private List<OzoneAcl> getAclList() {
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
        bucketInfo.getQuotaInNamespace()
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
        bucket.getQuotaInNamespace()))
        .collect(Collectors.toList());
  }

  @Override
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor,
      Map<String, String> metadata)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    if (checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName, type, factor);
    String requestId = UUID.randomUUID().toString();

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setType(HddsProtos.ReplicationType.valueOf(type.toString()))
        .setFactor(HddsProtos.ReplicationFactor.valueOf(factor.getValue()))
        .addAllMetadata(metadata)
        .setAcls(getAclList());

    if (Boolean.parseBoolean(metadata.get(OzoneConsts.GDPR_FLAG))) {
      try{
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
    return createOutputStream(openKey, requestId, type, factor);
  }

  private KeyProvider.KeyVersion getDEK(FileEncryptionInfo feInfo)
      throws IOException {
    // check crypto protocol version
    OzoneKMSUtil.checkCryptoProtocolVersion(feInfo);
    KeyProvider.KeyVersion decrypted;
    decrypted = OzoneKMSUtil.decryptEncryptedDataEncryptionKey(feInfo,
        getKeyProvider());
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
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    return getInputStreamWithRetryFunction(keyInfo);
  }

  @Override
  public void deleteKey(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    Preconditions.checkNotNull(keyName);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
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
    if(checkKeyNameEnabled){
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
        ReplicationType.valueOf(key.getType().toString()),
        key.getFactor().getNumber()))
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
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);

    List<OzoneKeyLocation> ozoneKeyLocations = new ArrayList<>();
    keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().forEach(
        (a) -> ozoneKeyLocations.add(new OzoneKeyLocation(a.getContainerID(),
            a.getLocalID(), a.getLength(), a.getOffset())));
    return new OzoneKeyDetails(keyInfo.getVolumeName(), keyInfo.getBucketName(),
        keyInfo.getKeyName(), keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(), ozoneKeyLocations, ReplicationType
        .valueOf(keyInfo.getType().toString()), keyInfo.getMetadata(),
        keyInfo.getFileEncryptionInfo(), keyInfo.getFactor().getNumber());
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG, ozoneManagerClient, xceiverClientManager);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
                                                String bucketName,
                                                String keyName,
                                                ReplicationType type,
                                                ReplicationFactor factor)
      throws IOException {
    verifyVolumeName(volumeName);
    verifyBucketName(bucketName);
    HddsClientUtils.checkNotNull(keyName, type, factor);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setType(HddsProtos.ReplicationType.valueOf(type.toString()))
        .setFactor(HddsProtos.ReplicationFactor.valueOf(factor.getValue()))
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
    if(checkKeyNameEnabled) {
      HddsClientUtils.verifyKeyName(keyName);
    }
    HddsClientUtils.checkNotNull(keyName, uploadID);
    Preconditions.checkArgument(partNumber > 0 && partNumber <=10000, "Part " +
        "number should be greater than zero and less than or equal to 10000");
    Preconditions.checkArgument(size >=0, "size should be greater than or " +
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
    KeyOutputStream keyOutputStream =
        new KeyOutputStream.Builder()
            .setHandler(openKey)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClient)
            .setRequestID(requestId)
            .setType(openKey.getKeyInfo().getType())
            .setFactor(openKey.getKeyInfo().getFactor())
            .setMultipartNumber(partNumber)
            .setMultipartUploadID(uploadID)
            .setIsMultipartKey(true)
            .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
            .setConfig(clientConfig)
            .build();
    keyOutputStream.addPreallocateBlocks(
        openKey.getKeyInfo().getLatestVersionLocations(),
        openKey.getOpenVersion());
    return new OzoneOutputStream(keyOutputStream);
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
        new OzoneMultipartUploadPartListParts(ReplicationType
            .fromProto(omMultipartUploadListParts.getReplicationType()),
            ReplicationFactor
                .fromProto(omMultipartUploadListParts.getReplicationFactor()),
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
            ReplicationType.fromProto(upload.getReplicationType()),
            ReplicationFactor.fromProto(upload.getReplicationFactor())))
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
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupFile(keyArgs);
    return getInputStreamWithRetryFunction(keyInfo);
  }

  /**
   * Create InputStream with Retry function to refresh pipeline information
   * if reads fail.
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
      String keyName, long size, ReplicationType type, ReplicationFactor factor,
      boolean overWrite, boolean recursive) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setType(HddsProtos.ReplicationType.valueOf(type.name()))
        .setFactor(HddsProtos.ReplicationFactor.valueOf(factor.getValue()))
        .setAcls(getAclList())
        .build();
    OpenKeySession keySession =
        ozoneManagerClient.createFile(keyArgs, overWrite, recursive);
    return createOutputStream(keySession, UUID.randomUUID().toString(), type,
        factor);
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey, long numEntries)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();
    return ozoneManagerClient
        .listStatus(keyArgs, recursive, startKey, numEntries);
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
    LengthInputStream lengthInputStream = KeyInputStream
        .getFromOmKeyInfo(keyInfo, xceiverClientManager,
            clientConfig.isChecksumVerify(), retryFunction);
    FileEncryptionInfo feInfo = keyInfo.getFileEncryptionInfo();
    if (feInfo != null) {
      final KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoInputStream cryptoIn =
          new CryptoInputStream(lengthInputStream.getWrappedStream(),
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneInputStream(cryptoIn);
    } else {
      try{
        GDPRSymmetricKey gk;
        Map<String, String> keyInfoMetadata = keyInfo.getMetadata();
        if(Boolean.valueOf(keyInfoMetadata.get(OzoneConsts.GDPR_FLAG))){
          gk = new GDPRSymmetricKey(
              keyInfoMetadata.get(OzoneConsts.GDPR_SECRET),
              keyInfoMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.DECRYPT_MODE, gk.getSecretKey());
          return new OzoneInputStream(
              new CipherInputStream(lengthInputStream, gk.getCipher()));
        }
      }catch (Exception ex){
        throw new IOException(ex);
      }
    }
    return new OzoneInputStream(lengthInputStream.getWrappedStream());
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey,
      String requestId, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    KeyOutputStream keyOutputStream =
        new KeyOutputStream.Builder()
            .setHandler(openKey)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClient)
            .setRequestID(requestId)
            .setType(HddsProtos.ReplicationType.valueOf(type.toString()))
            .setFactor(HddsProtos.ReplicationFactor.valueOf(factor.getValue()))
            .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
            .setConfig(clientConfig)
            .build();
    keyOutputStream
        .addPreallocateBlocks(openKey.getKeyInfo().getLatestVersionLocations(),
            openKey.getOpenVersion());
    final FileEncryptionInfo feInfo = keyOutputStream.getFileEncryptionInfo();
    if (feInfo != null) {
      KeyProvider.KeyVersion decrypted = getDEK(feInfo);
      final CryptoOutputStream cryptoOut =
          new CryptoOutputStream(keyOutputStream,
              OzoneKMSUtil.getCryptoCodec(conf, feInfo),
              decrypted.getMaterial(), feInfo.getIV());
      return new OzoneOutputStream(cryptoOut);
    } else {
      try{
        GDPRSymmetricKey gk;
        Map<String, String> openKeyMetadata =
            openKey.getKeyInfo().getMetadata();
        if(Boolean.valueOf(openKeyMetadata.get(OzoneConsts.GDPR_FLAG))){
          gk = new GDPRSymmetricKey(
              openKeyMetadata.get(OzoneConsts.GDPR_SECRET),
              openKeyMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.ENCRYPT_MODE, gk.getSecretKey());
          return new OzoneOutputStream(
              new CipherOutputStream(keyOutputStream, gk.getCipher()));
        }
      }catch (Exception ex){
        throw new IOException(ex);
      }

      return new OzoneOutputStream(keyOutputStream);
    }
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return OzoneKMSUtil.getKeyProvider(conf, getKeyProviderUri());
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

  @VisibleForTesting
  public OzoneManagerProtocol getOzoneManagerClient() {
    return ozoneManagerClient;
  }
}
