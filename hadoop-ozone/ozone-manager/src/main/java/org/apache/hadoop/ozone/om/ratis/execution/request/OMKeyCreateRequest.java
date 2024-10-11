/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis.execution.request;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.request.OMClientRequest.validateAndNormalizeKey;

/**
 * Handles CreateKey request.
 */
public class OMKeyCreateRequest extends OmRequestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateRequest.class);

  public OMKeyCreateRequest(OMRequest omRequest, OmBucketInfo bucketInfo) {
    super(omRequest, bucketInfo);
  }

  public OMRequest preProcess(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preProcess(ozoneManager);

    CreateKeyRequest createKeyRequest = request.getCreateKeyRequest();
    Preconditions.checkNotNull(createKeyRequest);

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    if (keyArgs.hasExpectedDataGeneration()) {
      ozoneManager.checkFeatureEnabled(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    }

    OmUtils.verifyKeyNameWithSnapshotReservedWord(keyArgs.getKeyName());
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
        .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
            OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if (checkKeyNameEnabled) {
      OmUtils.validateKeyName(keyArgs.getKeyName());
    }

    String keyPath = keyArgs.getKeyName();
    //OmBucketInfo bucketInfo = OmKeyUtils.resolveBucket(ozoneManager, keyArgs);
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(), keyPath, getBucketLayout());
    keyArgs = keyArgs.toBuilder().setVolumeName(getBucketInfo().getVolumeName())
        .setBucketName(getBucketInfo().getBucketName()).setKeyName(keyPath).setModificationTime(Time.now()).build();

    createKeyRequest = createKeyRequest.toBuilder().setKeyArgs(keyArgs).setClientID(UniqueId.next()).build();
    return request.toBuilder().setCreateKeyRequest(createKeyRequest).build();
  }

  public void authorize(OzoneManager ozoneManager) throws IOException {
    KeyArgs keyArgs = getOmRequest().getCreateKeyRequest().getKeyArgs();
    OmKeyUtils.checkKeyAcls(ozoneManager, keyArgs.getVolumeName(), keyArgs.getBucketName(), keyArgs.getKeyName(),
        IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY, getOmRequest());
  }
  
  public OMClientResponse process(OzoneManager ozoneManager, TermIndex termIndex) throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetrics omMetrics = ozoneManager.getMetrics();
    try {
      BucketLayout bucketLayout = getBucketLayout();
      OmBucketInfo bucketInfo = resolveBucket(ozoneManager, keyArgs.getVolumeName(), keyArgs.getBucketName());
      OMClientRequestUtils.checkClientRequestPrecondition(bucketInfo.getBucketLayout(), bucketLayout);

      // Check if Key already exists
    /*String dbKeyName = ozoneManager.getMetadataManager().getOzoneKey(keyArgs.getVolumeName(),
        keyArgs.getBucketName(), keyArgs.getKeyName());
    OmKeyInfo dbKeyInfo = ozoneManager.getMetadataManager().getKeyTable(getBucketLayout()).getIfExist(dbKeyName);
    validateAtomicRewrite(dbKeyInfo, keyArgs);*/

      // authorize
      OmKeyUtils.checkKeyAcls(ozoneManager, keyArgs.getVolumeName(), keyArgs.getBucketName(), keyArgs.getKeyName(),
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY, getOmRequest());

      // prepare
      FileEncryptionInfo encInfo;
      if (keyArgs.getIsMultipartKey()) {
        encInfo = OmKeyUtils.getFileEncryptionInfoForMpuKey(keyArgs, ozoneManager, bucketLayout);
      } else {
        encInfo = OmKeyUtils.getFileEncryptionInfo(ozoneManager, bucketInfo).orElse(null);
      }

      long trxnLogIndex = termIndex.getIndex();
      final ReplicationConfig repConfig = OzoneConfigUtil.resolveReplicationConfigPreference(keyArgs.getType(),
          keyArgs.getFactor(), keyArgs.getEcReplicationConfig(), bucketInfo.getDefaultReplicationConfig(),
          ozoneManager);
      OmKeyInfo omKeyInfo = OmKeyUtils.prepareKeyInfo(ozoneManager.getMetadataManager(), keyArgs, null,
          0, Collections.emptyList(), encInfo,
          ozoneManager.getPrefixManager(), bucketInfo, null, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          ozoneManager.isRatisEnabled(), repConfig);
      if (!keyArgs.getIsMultipartKey()) {
        addBlockInfo(ozoneManager, keyArgs, repConfig, omKeyInfo);
        // check bucket and volume quota
        OmKeyUtils.checkBucketQuotaInBytes(bucketInfo, omKeyInfo.getReplicatedSize());
      }

      // add changes
      long clientID = createKeyRequest.getClientID();
      String dbOpenKeyName = ozoneManager.getMetadataManager().getOpenKey(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), keyArgs.getKeyName(), clientID);
      CodecBuffer omKeyCodecBuffer = OmKeyInfo.getCodec(true).toDirectCodecBuffer(omKeyInfo);
      changeRecorder().add(ozoneManager.getMetadataManager().getOpenKeyTable(bucketLayout).getName(), dbOpenKeyName,
          omKeyCodecBuffer);

      // Prepare response
      OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
              .setKeyInfo(omKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(), keyArgs.getLatestVersionLocation()))
              .setID(clientID).setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateKey);
      omClientResponse = new DummyOMClientResponse(omResponse.build());
      omMetrics.incNumKeyAllocates();
    } catch (Exception ex) {
      omMetrics.incNumKeyAllocateFails();
      exception = ex;
      OMResponse rsp = OmKeyUtils.createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(getOmRequest()), ex);
      omClientResponse = new DummyOMClientResponse(rsp);
    }

    // Audit Log outside the lock
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.ALLOCATE_KEY, auditMap, exception,
        getOmRequest().getUserInfo()));

    logResult(createKeyRequest, omMetrics, exception, OMClientRequest.Result.SUCCESS, 0);
    return omClientResponse;
  }

  public void addBlockInfo(OzoneManager ozoneManager, KeyArgs keyArgs,
                       ReplicationConfig repConfig, OmKeyInfo omKeyInfo) throws IOException {
    long scmBlockSize = ozoneManager.getScmBlockSize();
    // NOTE size of a key is not a hard limit on anything, it is a value that
    // client should expect, in terms of current size of key. If client sets
    // a value, then this value is used, otherwise, we allocate a single
    // block which is the current size, if read by the client.
    final long requestedSize = keyArgs.getDataSize() > 0 ? keyArgs.getDataSize() : scmBlockSize;

    UserInfo userInfo = getOmRequest().getUserInfo();
    List<OmKeyLocationInfo> omKeyLocationInfoList = OmKeyUtils.allocateBlock(ozoneManager.getScmClient(),
            ozoneManager.getBlockTokenSecretManager(), repConfig,
            new ExcludeList(), requestedSize, scmBlockSize,
            ozoneManager.getPreallocateBlocksMax(), ozoneManager.isGrpcBlockTokenEnabled(),
            ozoneManager.getOMServiceId(), ozoneManager.getMetrics(),
            keyArgs.getSortDatanodes(), userInfo);
    // convert to proto and convert back as to filter out in existing logic
    List<OmKeyLocationInfo> newLocationList = omKeyLocationInfoList.stream()
        .map(info -> info.getProtobuf(false, getOmRequest().getVersion())).map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    omKeyInfo.appendNewBlocks(newLocationList, false);
    omKeyInfo.setDataSize(requestedSize + omKeyInfo.getDataSize());
  }

  protected void logResult(CreateKeyRequest createKeyRequest,
      OMMetrics omMetrics, Exception exception, OMClientRequest.Result result,
       int numMissingParents) {
    switch (result) {
    case SUCCESS:
      // Missing directories are created immediately, counting that here.
      // The metric for the key is incremented as part of the key commit.
      omMetrics.incNumKeys(numMissingParents);
      LOG.debug("Key created. Volume:{}, Bucket:{}, Key:{}",
              createKeyRequest.getKeyArgs().getVolumeName(),
              createKeyRequest.getKeyArgs().getBucketName(),
              createKeyRequest.getKeyArgs().getKeyName());
      break;
    case FAILURE:
      if (createKeyRequest.getKeyArgs().hasEcReplicationConfig()) {
        omMetrics.incEcKeyCreateFailsTotal();
      }
      LOG.error("Key creation failed. Volume:{}, Bucket:{}, Key:{}. ",
              createKeyRequest.getKeyArgs().getVolumeName(),
              createKeyRequest.getKeyArgs().getBucketName(),
              createKeyRequest.getKeyArgs().getKeyName(), exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCreateRequest: {}",
          createKeyRequest);
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateKey
  )
  public static OMRequest disallowCreateKeyWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCreateKeyRequest().getKeyArgs().hasEcReplicationConfig()) {
        throw new OMException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateKey
  )
  public static OMRequest blockCreateKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCreateKeyRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCreateKeyRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }

  /*protected void validateAtomicRewrite(OmKeyInfo dbKeyInfo, KeyArgs keyArgs)
      throws OMException {
    if (keyArgs.hasExpectedDataGeneration()) {
      // If a key does not exist, or if it exists but the updateID do not match, then fail this request.
      if (dbKeyInfo == null) {
        throw new OMException("Key not found during expected rewrite", OMException.ResultCodes.KEY_NOT_FOUND);
      }
      if (dbKeyInfo.getUpdateID() != keyArgs.getExpectedDataGeneration()) {
        throw new OMException("Generation mismatch during expected rewrite", OMException.ResultCodes.KEY_NOT_FOUND);
      }
    }
  }*/
}
