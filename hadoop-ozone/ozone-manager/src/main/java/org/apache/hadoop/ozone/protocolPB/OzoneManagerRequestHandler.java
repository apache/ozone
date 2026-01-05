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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.HBASE_SUPPORT;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusLightResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadInfo;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartInfo;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TransferLeadershipRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TransferLeadershipResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UpgradeFinalizationStatus;
import org.apache.hadoop.hdds.scm.protocolPB.OzonePBHelper;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelSnapshotDiffRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetObjectTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetObjectTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysLightResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListOpenFilesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListOpenFilesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListSnapshotDiffJobRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrintCompactionLogDagRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrintCompactionLogDagResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerBGSyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerBGSyncResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RefetchSecretKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSafeModeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSafeModeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command Handler for OM requests. OM State Machine calls this handler for
 * deserializing the client request and sending it to OM.
 */
public class OzoneManagerRequestHandler implements RequestHandler {
  static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerRequestHandler.class);
  private final OzoneManager impl;
  private FaultInjector injector;

  public OzoneManagerRequestHandler(OzoneManager om) {
    this.impl = om;
  }

  //TODO simplify it to make it shorter
  @SuppressWarnings("methodlength")
  @Override
  public OMResponse handleReadRequest(OMRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received OMRequest: {}, ", request);
    }
    Type cmdType = request.getCmdType();
    OMResponse.Builder responseBuilder = OmResponseUtil.getOMResponseBuilder(
        request);
    try {
      switch (cmdType) {
      case CheckVolumeAccess:
        CheckVolumeAccessResponse checkVolumeAccessResponse = checkVolumeAccess(
            request.getCheckVolumeAccessRequest());
        responseBuilder.setCheckVolumeAccessResponse(checkVolumeAccessResponse);
        break;
      case InfoVolume:
        InfoVolumeResponse infoVolumeResponse = infoVolume(
            request.getInfoVolumeRequest());
        responseBuilder.setInfoVolumeResponse(infoVolumeResponse);
        break;
      case ListVolume:
        ListVolumeResponse listVolumeResponse = listVolumes(
            request.getListVolumeRequest());
        responseBuilder.setListVolumeResponse(listVolumeResponse);
        break;
      case InfoBucket:
        InfoBucketResponse infoBucketResponse = infoBucket(
            request.getInfoBucketRequest());
        responseBuilder.setInfoBucketResponse(infoBucketResponse);
        break;
      case ListBuckets:
        ListBucketsResponse listBucketsResponse = listBuckets(
            request.getListBucketsRequest());
        responseBuilder.setListBucketsResponse(listBucketsResponse);
        break;
      case LookupKey:
        LookupKeyResponse lookupKeyResponse = lookupKey(
            request.getLookupKeyRequest(), request.getVersion());
        responseBuilder.setLookupKeyResponse(lookupKeyResponse);
        break;
      case ListKeys:
        ListKeysResponse listKeysResponse = listKeys(
            request.getListKeysRequest(), request.getVersion());
        responseBuilder.setListKeysResponse(listKeysResponse);
        break;
      case ListKeysLight:
        ListKeysLightResponse listKeysLightResponse = listKeysLight(
            request.getListKeysRequest());
        responseBuilder.setListKeysLightResponse(listKeysLightResponse);
        break;
      case ListMultiPartUploadParts:
        MultipartUploadListPartsResponse listPartsResponse =
            listParts(request.getListMultipartUploadPartsRequest());
        responseBuilder.setListMultipartUploadPartsResponse(listPartsResponse);
        break;
      case ListMultipartUploads:
        ListMultipartUploadsResponse response =
            listMultipartUploads(request.getListMultipartUploadsRequest());
        responseBuilder.setListMultipartUploadsResponse(response);
        break;
      case ListOpenFiles:
        ListOpenFilesResponse listOpenFilesResponse = listOpenFiles(
            request.getListOpenFilesRequest(), request.getVersion());
        responseBuilder.setListOpenFilesResponse(listOpenFilesResponse);
        break;
      case ServiceList:
        ServiceListResponse serviceListResponse = getServiceList(
            request.getServiceListRequest());
        responseBuilder.setServiceListResponse(serviceListResponse);
        break;
      case RangerBGSync:
        RangerBGSyncResponse rangerBGSyncResponse = triggerRangerBGSync(
            request.getRangerBGSyncRequest());
        responseBuilder.setRangerBGSyncResponse(rangerBGSyncResponse);
        break;
      case DBUpdates:
        DBUpdatesResponse dbUpdatesResponse = getOMDBUpdates(
            request.getDbUpdatesRequest());
        responseBuilder.setDbUpdatesResponse(dbUpdatesResponse);
        break;
      case GetFileStatus:
        GetFileStatusResponse getFileStatusResponse = getOzoneFileStatus(
            request.getGetFileStatusRequest(), request.getVersion());
        responseBuilder.setGetFileStatusResponse(getFileStatusResponse);
        break;
      case LookupFile:
        LookupFileResponse lookupFileResponse =
            lookupFile(request.getLookupFileRequest(), request.getVersion());
        responseBuilder.setLookupFileResponse(lookupFileResponse);
        break;
      case ListStatus:
        ListStatusResponse listStatusResponse =
            listStatus(request.getListStatusRequest(), request.getVersion());
        responseBuilder.setListStatusResponse(listStatusResponse);
        break;
      case ListStatusLight:
        ListStatusLightResponse listStatusLightResponse =
            listStatusLight(request.getListStatusRequest(),
                request.getVersion());
        responseBuilder.setListStatusLightResponse(listStatusLightResponse);
        break;
      case GetAcl:
        GetAclResponse getAclResponse =
            getAcl(request.getGetAclRequest());
        responseBuilder.setGetAclResponse(getAclResponse);
        break;
      case FinalizeUpgradeProgress:
        FinalizeUpgradeProgressResponse upgradeProgressResponse =
            reportUpgradeProgress(request.getFinalizeUpgradeProgressRequest());
        responseBuilder
            .setFinalizeUpgradeProgressResponse(upgradeProgressResponse);
        break;
      case PrepareStatus:
        PrepareStatusResponse prepareStatusResponse = getPrepareStatus();
        responseBuilder.setPrepareStatusResponse(prepareStatusResponse);
        break;
      case GetS3VolumeContext:
        GetS3VolumeContextResponse s3VolumeContextResponse =
            getS3VolumeContext();
        responseBuilder.setGetS3VolumeContextResponse(s3VolumeContextResponse);
        break;
      case TenantGetUserInfo:
        impl.checkS3MultiTenancyEnabled();
        TenantGetUserInfoResponse getUserInfoResponse = tenantGetUserInfo(
            request.getTenantGetUserInfoRequest());
        responseBuilder.setTenantGetUserInfoResponse(getUserInfoResponse);
        break;
      case ListTenant:
        impl.checkS3MultiTenancyEnabled();
        ListTenantResponse listTenantResponse = listTenant(
            request.getListTenantRequest());
        responseBuilder.setListTenantResponse(listTenantResponse);
        break;
      case TenantListUser:
        impl.checkS3MultiTenancyEnabled();
        TenantListUserResponse listUserResponse = tenantListUsers(
            request.getTenantListUserRequest());
        responseBuilder.setTenantListUserResponse(listUserResponse);
        break;
      case GetKeyInfo:
        responseBuilder.setGetKeyInfoResponse(
            getKeyInfo(request.getGetKeyInfoRequest(), request.getVersion()));
        break;
      case ListSnapshot:
        OzoneManagerProtocolProtos.ListSnapshotResponse listSnapshotResponse =
            getSnapshots(request.getListSnapshotRequest());
        responseBuilder.setListSnapshotResponse(listSnapshotResponse);
        break;
      case SnapshotDiff:
        SnapshotDiffResponse snapshotDiffReport = snapshotDiff(
            request.getSnapshotDiffRequest());
        responseBuilder.setSnapshotDiffResponse(snapshotDiffReport);
        break;
      case CancelSnapshotDiff:
        CancelSnapshotDiffResponse cancelSnapshotDiff = cancelSnapshotDiff(
                request.getCancelSnapshotDiffRequest());
        responseBuilder.setCancelSnapshotDiffResponse(cancelSnapshotDiff);
        break;
      case ListSnapshotDiffJobs:
        ListSnapshotDiffJobResponse listSnapDiffResponse =
            listSnapshotDiffJobs(request.getListSnapshotDiffJobRequest());
        responseBuilder.setListSnapshotDiffJobResponse(listSnapDiffResponse);
        break;
      case EchoRPC:
        EchoRPCResponse echoRPCResponse =
            echoRPC(request.getEchoRPCRequest());
        responseBuilder.setEchoRPCResponse(echoRPCResponse);
        break;
      case TransferLeadership:
        responseBuilder.setTransferOmLeadershipResponse(transferLeadership(
            request.getTransferOmLeadershipRequest()));
        break;
      case RefetchSecretKey:
        responseBuilder.setRefetchSecretKeyResponse(refetchSecretKey());
        break;
      case SetSafeMode:
        SetSafeModeResponse setSafeModeResponse =
            setSafeMode(request.getSetSafeModeRequest());
        responseBuilder.setSetSafeModeResponse(setSafeModeResponse);
        break;
      case PrintCompactionLogDag:
        PrintCompactionLogDagResponse printCompactionLogDagResponse =
            printCompactionLogDag(request.getPrintCompactionLogDagRequest());
        responseBuilder
            .setPrintCompactionLogDagResponse(printCompactionLogDagResponse);
        break;
      case GetSnapshotInfo:
        OzoneManagerProtocolProtos.SnapshotInfoResponse snapshotInfoResponse =
            getSnapshotInfo(request.getSnapshotInfoRequest());
        responseBuilder.setSnapshotInfoResponse(snapshotInfoResponse);
        break;
      case GetQuotaRepairStatus:
        OzoneManagerProtocolProtos.GetQuotaRepairStatusResponse quotaRepairStatusRsp =
            getQuotaRepairStatus(request.getGetQuotaRepairStatusRequest());
        responseBuilder.setGetQuotaRepairStatusResponse(quotaRepairStatusRsp);
        break;
      case StartQuotaRepair:
        OzoneManagerProtocolProtos.StartQuotaRepairResponse startQuotaRepairRsp =
            startQuotaRepair(request.getStartQuotaRepairRequest());
        responseBuilder.setStartQuotaRepairResponse(startQuotaRepairRsp);
        break;
      case GetObjectTagging:
        OzoneManagerProtocolProtos.GetObjectTaggingResponse getObjectTaggingResponse =
            getObjectTagging(request.getGetObjectTaggingRequest());
        responseBuilder.setGetObjectTaggingResponse(getObjectTaggingResponse);
        break;
      case GetLifecycleConfiguration:
        impl.checkLifecycleEnabled();
        GetLifecycleConfigurationResponse getLifecycleConfigurationResponse =
            infoLifecycleConfiguration(
                request.getGetLifecycleConfigurationRequest());
        responseBuilder.setGetLifecycleConfigurationResponse(
            getLifecycleConfigurationResponse);
        break;
      case GetLifecycleServiceStatus:
        impl.checkLifecycleEnabled();
        GetLifecycleServiceStatusResponse getLifecycleServiceStatusResponse =
            impl.getLifecycleServiceStatus();
        responseBuilder.setGetLifecycleServiceStatusResponse(
            getLifecycleServiceStatusResponse);
        break;
      default:
        responseBuilder.setSuccess(false);
        responseBuilder.setMessage("Unrecognized Command Type: " + cmdType);
        break;
      }
      responseBuilder.setSuccess(true);
    } catch (IOException ex) {
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(exceptionToResponseStatus(ex));
      if (ex.getMessage() != null) {
        responseBuilder.setMessage(ex.getMessage());
      }
    }
    return responseBuilder.build();
  }

  @Override
  public OMClientResponse handleWriteRequestImpl(OMRequest omRequest, ExecutionContext context) throws IOException {
    injectPause();
    OMClientRequest omClientRequest =
        OzoneManagerRatisUtils.createClientRequest(omRequest, impl);
    try {
      OMClientResponse omClientResponse = captureLatencyNs(
          impl.getPerfMetrics().getValidateAndUpdateCacheLatencyNs(),
          () -> Objects.requireNonNull(omClientRequest.validateAndUpdateCache(getOzoneManager(), context),
              "omClientResponse returned by validateAndUpdateCache cannot be null"));
      OMAuditLogger.log(omClientRequest.getAuditBuilder(), context.getTermIndex());
      return omClientResponse;
    } catch (Throwable th) {
      OMAuditLogger.log(omClientRequest.getAuditBuilder(), omClientRequest, getOzoneManager(), context.getTermIndex(),
          th);
      throw th;
    }
  }

  @VisibleForTesting
  public void setInjector(FaultInjector injector) {
    this.injector = injector;
  }

  @VisibleForTesting
  public FaultInjector getInjector() {
    return injector;
  }

  /**
   * Inject pause for test only.
   *
   * @throws IOException
   */
  private void injectPause() throws IOException {
    if (injector != null) {
      injector.pause();
    }
  }

  private DBUpdatesResponse getOMDBUpdates(
      DBUpdatesRequest dbUpdatesRequest)
      throws IOException {

    DBUpdatesResponse.Builder builder = DBUpdatesResponse
        .newBuilder();
    DBUpdates dbUpdatesWrapper =
        impl.getDBUpdates(dbUpdatesRequest);
    for (int i = 0; i < dbUpdatesWrapper.getData().size(); i++) {
      builder.addData(OzonePBHelper.getByteString(
          dbUpdatesWrapper.getData().get(i)));
    }
    builder.setSequenceNumber(dbUpdatesWrapper.getCurrentSequenceNumber());
    builder.setLatestSequenceNumber(dbUpdatesWrapper.getLatestSequenceNumber());
    builder.setDbUpdateSuccess(dbUpdatesWrapper.isDBUpdateSuccess());
    return builder.build();
  }

  private GetAclResponse getAcl(GetAclRequest req) throws IOException {
    List<OzoneAclInfo> acls = new ArrayList<>();
    List<OzoneAcl> aclList =
        impl.getAcl(OzoneObjInfo.fromProtobuf(req.getObj()));
    if (aclList != null) {
      aclList.forEach(a -> acls.add(OzoneAcl.toProtobuf(a)));
    }
    return GetAclResponse.newBuilder().addAllAcls(acls).build();
  }

  // Convert and exception to corresponding status code

  protected Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof OMException) {
      return Status.values()[((OMException) ex).getResult().ordinal()];
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unknown error occurs", ex);
      }
      return Status.INTERNAL_ERROR;
    }
  }

  /**
   * Validates that the incoming OM request has required parameters.
   * TODO: Add more validation checks before writing the request to Ratis log.
   *
   * @param omRequest client request to OM
   * @throws OMException thrown if required parameters are set to null.
   */
  @Override
  public void validateRequest(OMRequest omRequest) throws OMException {
    Type cmdType = omRequest.getCmdType();
    if (cmdType == null) {
      throw new OMException("CmdType is null",
          OMException.ResultCodes.INVALID_REQUEST);
    }
    if (omRequest.getClientId() == null) {
      throw new OMException("ClientId is null",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // Layout version should have been set up the leader while serializing
    // the request, and hence cannot be null. This version is used by each
    // node to identify which request handler version to use.
    if (omRequest.getLayoutVersion() == null) {
      throw new OMException("LayoutVersion for request is null.",
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  private CheckVolumeAccessResponse checkVolumeAccess(
      CheckVolumeAccessRequest request) throws IOException {
    CheckVolumeAccessResponse.Builder resp =
        CheckVolumeAccessResponse.newBuilder();
    boolean access = impl.checkVolumeAccess(request.getVolumeName(),
        request.getUserAcl());
    // if no access, set the response status as access denied

    if (!access) {
      throw new OMException(OMException.ResultCodes.ACCESS_DENIED);
    }

    return resp.build();
  }

  private InfoVolumeResponse infoVolume(InfoVolumeRequest request)
      throws IOException {
    InfoVolumeResponse.Builder resp = InfoVolumeResponse.newBuilder();
    String volume = request.getVolumeName();

    OmVolumeArgs ret = impl.getVolumeInfo(volume);
    resp.setVolumeInfo(ret.getProtobuf());

    return resp.build();
  }

  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  private TenantGetUserInfoResponse tenantGetUserInfo(
      TenantGetUserInfoRequest request) throws IOException {

    final TenantGetUserInfoResponse.Builder resp =
        TenantGetUserInfoResponse.newBuilder();
    final String userPrincipal = request.getUserPrincipal();

    TenantUserInfoValue ret = impl.tenantGetUserInfo(userPrincipal);
    // Note impl.tenantGetUserInfo() throws if errs
    if (ret != null) {
      resp.addAllAccessIdInfo(ret.getAccessIdInfoList());
    }

    return resp.build();
  }

  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  private TenantListUserResponse tenantListUsers(
      TenantListUserRequest request) throws IOException {
    TenantListUserResponse.Builder builder =
        TenantListUserResponse.newBuilder();
    TenantUserList usersInTenant =
        impl.listUsersInTenant(request.getTenantId(), request.getPrefix());
    // Note impl.listUsersInTenant() throws if errs
    if (usersInTenant != null) {
      builder.addAllUserAccessIdInfo(usersInTenant.getUserAccessIds());
    }
    return builder.build();
  }

  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  private ListTenantResponse listTenant(
      ListTenantRequest request) throws IOException {

    final ListTenantResponse.Builder resp = ListTenantResponse.newBuilder();

    TenantStateList ret = impl.listTenant();
    resp.addAllTenantState(ret.getTenantStateList());

    return resp.build();
  }

  private ListVolumeResponse listVolumes(ListVolumeRequest request)
      throws IOException {
    ListVolumeResponse.Builder resp = ListVolumeResponse.newBuilder();
    List<OmVolumeArgs> result = Lists.newArrayList();

    if (request.getScope()
        == ListVolumeRequest.Scope.VOLUMES_BY_USER) {
      result = impl.listVolumeByUser(request.getUserName(),
          request.getPrefix(), request.getPrevKey(), request.getMaxKeys());
    } else if (request.getScope()
        == ListVolumeRequest.Scope.VOLUMES_BY_CLUSTER) {
      result =
          impl.listAllVolumes(request.getPrefix(), request.getPrevKey(),
              request.getMaxKeys());
    }

    result.forEach(item -> resp.addVolumeInfo(item.getProtobuf()));

    return resp.build();
  }

  private InfoBucketResponse infoBucket(InfoBucketRequest request)
      throws IOException {
    InfoBucketResponse.Builder resp =
        InfoBucketResponse.newBuilder();
    OmBucketInfo omBucketInfo = impl.getBucketInfo(
        request.getVolumeName(), request.getBucketName());
    resp.setBucketInfo(omBucketInfo.getProtobuf());

    return resp.build();
  }

  private LookupKeyResponse lookupKey(LookupKeyRequest request,
      int clientVersion) throws IOException {
    LookupKeyResponse.Builder resp =
        LookupKeyResponse.newBuilder();
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setLatestVersionLocation(keyArgs.getLatestVersionLocation())
        .setSortDatanodesInPipeline(keyArgs.getSortDatanodes())
        .setHeadOp(keyArgs.getHeadOp())
        .build();
    OmKeyInfo keyInfo = impl.lookupKey(omKeyArgs);

    resp.setKeyInfo(keyInfo.getProtobuf(keyArgs.getHeadOp(), clientVersion));

    return resp.build();
  }

  private GetKeyInfoResponse getKeyInfo(GetKeyInfoRequest request,
                                        int clientVersion) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setLatestVersionLocation(keyArgs.getLatestVersionLocation())
        .setSortDatanodesInPipeline(keyArgs.getSortDatanodes())
        .setHeadOp(keyArgs.getHeadOp())
        .setForceUpdateContainerCacheFromSCM(
            keyArgs.getForceUpdateContainerCacheFromSCM())
        .setMultipartUploadPartNumber(keyArgs.getMultipartNumber())
        .build();
    KeyInfoWithVolumeContext keyInfo = impl.getKeyInfo(omKeyArgs,
        request.getAssumeS3Context());

    return keyInfo.toProtobuf(clientVersion);
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.LookupKey
  )
  public static OMResponse disallowLookupKeyResponseWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasLookupKeyResponse()) {
      return resp;
    }
    if (resp.getLookupKeyResponse().getKeyInfo().hasEcReplicationConfig()) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("Key is a key with Erasure Coded replication, which"
              + " the client can not understand.\n"
              + "Please upgrade the client before trying to read the key: "
              + req.getLookupKeyRequest().getKeyArgs().getVolumeName()
              + "/" + req.getLookupKeyRequest().getKeyArgs().getBucketName()
              + "/" + req.getLookupKeyRequest().getKeyArgs().getKeyName()
              + ".")
          .clearLookupKeyResponse()
          .build();
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.LookupKey
  )
  public static OMResponse disallowLookupKeyWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasLookupKeyResponse()) {
      return resp;
    }
    KeyInfo keyInfo = resp.getLookupKeyResponse().getKeyInfo();
    // If the key is present inside a bucket using a non LEGACY bucket layout,
    // then the client needs to be upgraded before proceeding.
    if (keyInfo.hasVolumeName() && keyInfo.hasBucketName() &&
        !ctx.getBucketLayout(keyInfo.getVolumeName(), keyInfo.getBucketName())
            .equals(BucketLayout.LEGACY)) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("Key is present inside a bucket with bucket layout " +
              "features, which the client can not understand. Please upgrade" +
              " the client to a compatible version before trying to read the" +
              " key info for "
              + req.getLookupKeyRequest().getKeyArgs().getVolumeName()
              + "/" + req.getLookupKeyRequest().getKeyArgs().getBucketName()
              + "/" + req.getLookupKeyRequest().getKeyArgs().getKeyName()
              + ".")
          .clearLookupKeyResponse()
          .build();
    }
    return resp;
  }

  private ListBucketsResponse listBuckets(ListBucketsRequest request)
      throws IOException {
    ListBucketsResponse.Builder resp =
        ListBucketsResponse.newBuilder();

    List<OmBucketInfo> buckets = impl.listBuckets(
        request.getVolumeName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount(),
        request.getHasSnapshot());
    for (OmBucketInfo bucket : buckets) {
      resp.addBucketInfo(bucket.getProtobuf());
    }

    return resp.build();
  }

  private ListKeysResponse listKeys(ListKeysRequest request, int clientVersion)
      throws IOException {
    ListKeysResponse.Builder resp =
        ListKeysResponse.newBuilder();

    ListKeysResult listKeysResult = impl.listKeys(
        request.getVolumeName(),
        request.getBucketName(),
        request.getStartKey(),
        request.getPrefix(),
        limitListSizeInt(request.getCount()));
    for (OmKeyInfo key : listKeysResult.getKeys()) {
      resp.addKeyInfo(key.getProtobuf(true, clientVersion));
    }
    resp.setIsTruncated(listKeysResult.isTruncated());
    return resp.build();
  }

  private ListKeysLightResponse listKeysLight(ListKeysRequest request)
      throws IOException {
    ListKeysLightResponse.Builder resp =
        ListKeysLightResponse.newBuilder();

    ListKeysLightResult listKeysLightResult = impl.listKeysLight(
        request.getVolumeName(),
        request.getBucketName(),
        request.getStartKey(),
        request.getPrefix(),
        limitListSizeInt(request.getCount()));
    for (BasicOmKeyInfo key : listKeysLightResult.getKeys()) {
      resp.addBasicKeyInfo(key.getProtobuf());
    }
    resp.setIsTruncated(listKeysLightResult.isTruncated());
    return resp.build();
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListKeys
  )
  public static OMResponse disallowListKeysResponseWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasListKeysResponse()) {
      return resp;
    }
    List<KeyInfo> keys = resp.getListKeysResponse().getKeyInfoList();
    for (KeyInfo key : keys) {
      if (key.hasEcReplicationConfig()) {
        resp = resp.toBuilder()
            .setStatus(Status.NOT_SUPPORTED_OPERATION)
            .setMessage("The list of keys contains keys with Erasure Coded"
                + " replication set, hence the client is not able to"
                + " represent all the keys returned. Please upgrade the"
                + " client to get the list of keys.")
            .clearListKeysResponse()
            .build();
      }
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListKeys
  )
  public static OMResponse disallowListKeysWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasListKeysResponse()) {
      return resp;
    }

    // Put volume and bucket pairs into a set to avoid duplicates.
    HashSet<Pair<String, String>> volumeBucketSet = new HashSet<>();
    List<KeyInfo> keys = resp.getListKeysResponse().getKeyInfoList();
    for (KeyInfo key : keys) {
      if (key.hasVolumeName() && key.hasBucketName()) {
        volumeBucketSet.add(
            new ImmutablePair<>(key.getVolumeName(), key.getBucketName()));
      }
    }

    for (Pair<String, String> volumeBucket : volumeBucketSet) {
      // If any of the buckets have a non legacy layout, then the client is
      // not compatible with the response.
      if (!ctx.getBucketLayout(volumeBucket.getLeft(), volumeBucket.getRight())
          .isLegacy()) {
        resp = resp.toBuilder()
            .setStatus(Status.NOT_SUPPORTED_OPERATION)
            .setMessage("The list of keys contains keys present inside bucket" +
                " with bucket layout features, hence the client is not able " +
                "to understand all the keys returned. Please upgrade the"
                + " client to get the list of keys.")
            .clearListKeysResponse()
            .build();
        break;
      }
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListTrash
  )
  public static OMResponse disallowListTrashWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasListTrashResponse()) {
      return resp;
    }
    List<RepeatedKeyInfo> repeatedKeys =
        resp.getListTrashResponse().getDeletedKeysList();
    for (RepeatedKeyInfo repeatedKey : repeatedKeys) {
      for (KeyInfo key : repeatedKey.getKeyInfoList()) {
        if (key.hasEcReplicationConfig()) {
          resp = resp.toBuilder()
              .setStatus(Status.NOT_SUPPORTED_OPERATION)
              .setMessage("The list of keys contains keys with Erasure Coded"
                  + " replication set, hence the client is not able to"
                  + " represent all the keys returned. Please upgrade the"
                  + " client to get the list of keys.")
              .clearListTrashResponse()
              .build();
        }
      }
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListTrash
  )
  public static OMResponse disallowListTrashWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasListTrashResponse()) {
      return resp;
    }

    // Add the volume and bucket pairs to a set to avoid duplicates.
    List<RepeatedKeyInfo> repeatedKeys =
        resp.getListTrashResponse().getDeletedKeysList();
    HashSet<Pair<String, String>> volumeBucketSet = new HashSet<>();

    for (RepeatedKeyInfo repeatedKey : repeatedKeys) {
      for (KeyInfo key : repeatedKey.getKeyInfoList()) {
        if (key.hasVolumeName() && key.hasBucketName()) {
          volumeBucketSet.add(
              new ImmutablePair<>(key.getVolumeName(), key.getBucketName()));
        }
      }
    }

    // If any of the keys is present inside a bucket using a non LEGACY bucket
    // layout, then the client needs to be upgraded before proceeding.
    for (Pair<String, String> volumeBucket : volumeBucketSet) {
      if (!ctx.getBucketLayout(volumeBucket.getLeft(), volumeBucket.getRight())
          .isLegacy()) {
        resp = resp.toBuilder()
            .setStatus(Status.NOT_SUPPORTED_OPERATION)
            .setMessage("The list of keys contains keys present in buckets " +
                " using bucket layout features, hence the client is not able to"
                + " understand all the keys returned. Please upgrade the"
                + " client to get the list of keys.")
            .clearListTrashResponse()
            .build();
        break;
      }
    }
    return resp;
  }

  @DisallowedUntilLayoutVersion(HBASE_SUPPORT)
  private ListOpenFilesResponse listOpenFiles(ListOpenFilesRequest req,
                                              int clientVersion)
      throws IOException {
    ListOpenFilesResponse.Builder resp = ListOpenFilesResponse.newBuilder();

    ListOpenFilesResult res =
        impl.listOpenFiles(req.getPath(), limitListSizeInt(req.getCount()), req.getToken());
    // TODO: Is there a clean way to avoid ser-de for responses:
    //  OM does: ListOpenFilesResult -> ListOpenFilesResponse
    //  Client : ListOpenFilesResponse -> ListOpenFilesResult

    resp.setTotalOpenKeyCount(res.getTotalOpenKeyCount());
    resp.setHasMore(res.hasMore());
    if (res.getContinuationToken() != null) {
      resp.setContinuationToken(res.getContinuationToken());
    }

    for (OpenKeySession e : res.getOpenKeys()) {
      resp.addClientID(e.getId());
      resp.addKeyInfo(e.getKeyInfo().getProtobuf(clientVersion));
    }

    return resp.build();
  }

  private ServiceListResponse getServiceList(ServiceListRequest request)
      throws IOException {
    ServiceListResponse.Builder resp = ServiceListResponse.newBuilder();

    ServiceInfoEx serviceInfoEx = impl.getServiceInfo();

    List<OzoneManagerProtocolProtos.ServiceInfo> serviceInfoProtos =
        new ArrayList<>();
    List<ServiceInfo> serviceInfos = serviceInfoEx.getServiceInfoList();
    for (ServiceInfo info : serviceInfos) {
      serviceInfoProtos.add(info.getProtobuf());
    }

    resp.addAllServiceInfo(serviceInfoProtos);
    if (serviceInfoEx.getCaCertificate() != null) {
      resp.setCaCertificate(serviceInfoEx.getCaCertificate());
    }

    for (String ca : serviceInfoEx.getCaCertPemList()) {
      resp.addCaCerts(ca);
    }

    return resp.build();
  }

  private MultipartUploadListPartsResponse listParts(
      MultipartUploadListPartsRequest multipartUploadListPartsRequest)
      throws IOException {

    MultipartUploadListPartsResponse.Builder response =
        MultipartUploadListPartsResponse.newBuilder();

    OmMultipartUploadListParts omMultipartUploadListParts =
        impl.listParts(multipartUploadListPartsRequest.getVolume(),
            multipartUploadListPartsRequest.getBucket(),
            multipartUploadListPartsRequest.getKey(),
            multipartUploadListPartsRequest.getUploadID(),
            multipartUploadListPartsRequest.getPartNumbermarker(),
            multipartUploadListPartsRequest.getMaxParts());

    List<OmPartInfo> omPartInfoList =
        omMultipartUploadListParts.getPartInfoList();

    List<PartInfo> partInfoList =
        new ArrayList<>();

    omPartInfoList.forEach(partInfo -> partInfoList.add(partInfo.getProto()));

    HddsProtos.ReplicationType repType = omMultipartUploadListParts
        .getReplicationConfig()
        .getReplicationType();
    response.setType(repType);
    if (repType == HddsProtos.ReplicationType.EC) {
      response.setEcReplicationConfig(
          ((ECReplicationConfig)omMultipartUploadListParts
              .getReplicationConfig()).toProto());
    } else {
      response.setFactor(ReplicationConfig.getLegacyFactor(
          omMultipartUploadListParts.getReplicationConfig()));
    }
    response.setNextPartNumberMarker(
        omMultipartUploadListParts.getNextPartNumberMarker());
    response.setIsTruncated(omMultipartUploadListParts.isTruncated());

    return response.addAllPartsList(partInfoList).build();


  }

  private ListMultipartUploadsResponse listMultipartUploads(
      ListMultipartUploadsRequest request)
      throws IOException {

    OmMultipartUploadList omMultipartUploadList = impl.listMultipartUploads(request.getVolume(), request.getBucket(),
        request.getPrefix(),
        request.getKeyMarker(), request.getUploadIdMarker(), request.getMaxUploads(), request.getWithPagination());

    List<MultipartUploadInfo> info = omMultipartUploadList
        .getUploads()
        .stream()
        .map(upload -> {
          MultipartUploadInfo.Builder bldr = MultipartUploadInfo.newBuilder()
              .setVolumeName(upload.getVolumeName())
              .setBucketName(upload.getBucketName())
              .setKeyName(upload.getKeyName())
              .setUploadId(upload.getUploadId());

          HddsProtos.ReplicationType repType = upload.getReplicationConfig()
              .getReplicationType();
          bldr.setType(repType);
          if (repType == HddsProtos.ReplicationType.EC) {
            bldr.setEcReplicationConfig(
                ((ECReplicationConfig)upload.getReplicationConfig())
                    .toProto());
          } else {
            bldr.setFactor(ReplicationConfig.getLegacyFactor(
                upload.getReplicationConfig()));
          }
          bldr.setCreationTime(upload.getCreationTime().toEpochMilli());
          return bldr.build();
        })
        .collect(Collectors.toList());

    ListMultipartUploadsResponse response =
        ListMultipartUploadsResponse.newBuilder()
            .addAllUploadsList(info)
            .setIsTruncated(omMultipartUploadList.isTruncated())
            .setNextKeyMarker(omMultipartUploadList.getNextKeyMarker())
            .setNextUploadIdMarker(omMultipartUploadList.getNextUploadIdMarker())
            .build();

    return response;
  }

  private GetFileStatusResponse getOzoneFileStatus(
      GetFileStatusRequest request, int clientVersion) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();

    GetFileStatusResponse.Builder rb = GetFileStatusResponse.newBuilder();
    rb.setStatus(impl.getFileStatus(omKeyArgs).getProtobuf(clientVersion));

    return rb.build();
  }

  private RangerBGSyncResponse triggerRangerBGSync(
      RangerBGSyncRequest rangerBGSyncRequest) throws IOException {

    boolean res = impl.triggerRangerBGSync(rangerBGSyncRequest.getNoWait());

    return RangerBGSyncResponse.newBuilder().setRunSuccess(res).build();
  }

  private RefetchSecretKeyResponse refetchSecretKey() {
    UUID uuid = impl.refetchSecretKey();
    RefetchSecretKeyResponse response =
        RefetchSecretKeyResponse.newBuilder()
            .setId(ProtobufUtils.toProtobuf(uuid)).build();
    return response;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.GetFileStatus
  )
  public static OMResponse disallowGetFileStatusWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasGetFileStatusResponse()) {
      return resp;
    }
    if (resp.getGetFileStatusResponse().getStatus().getKeyInfo()
        .hasEcReplicationConfig()) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("Key is a key with Erasure Coded replication, which"
              + " the client can not understand."
              + " Please upgrade the client before trying to read the key info"
              + " for "
              + req.getGetFileStatusRequest().getKeyArgs().getVolumeName()
              + "/" + req.getGetFileStatusRequest().getKeyArgs().getBucketName()
              + "/" + req.getGetFileStatusRequest().getKeyArgs().getKeyName()
              + ".")
          .clearGetFileStatusResponse()
          .build();
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.GetFileStatus
  )
  public static OMResponse disallowGetFileStatusWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasGetFileStatusResponse()) {
      return resp;
    }

    // If the File is present inside a bucket with non LEGACY layout,
    // then the client should be upgraded before proceeding.
    KeyInfo keyInfo = resp.getGetFileStatusResponse().getStatus().getKeyInfo();
    if (keyInfo.hasVolumeName() && keyInfo.hasBucketName() &&
        !ctx.getBucketLayout(keyInfo.getVolumeName(), keyInfo.getBucketName())
            .isLegacy()) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("Key is present in a bucket using bucket layout features"
              + " which the client can not understand."
              + " Please upgrade the client before trying to read the key info"
              + " for "
              + req.getGetFileStatusRequest().getKeyArgs().getVolumeName()
              + "/" + req.getGetFileStatusRequest().getKeyArgs().getBucketName()
              + "/" + req.getGetFileStatusRequest().getKeyArgs().getKeyName()
              + ".")
          .clearGetFileStatusResponse()
          .build();
    }
    return resp;
  }

  private LookupFileResponse lookupFile(LookupFileRequest request,
      int clientVersion) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setSortDatanodesInPipeline(keyArgs.getSortDatanodes())
        .setLatestVersionLocation(keyArgs.getLatestVersionLocation())
        .build();
    return LookupFileResponse.newBuilder()
        .setKeyInfo(impl.lookupFile(omKeyArgs).getProtobuf(clientVersion))
        .build();
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.LookupFile
  )
  public static OMResponse disallowLookupFileWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasLookupFileResponse()) {
      return resp;
    }
    if (resp.getLookupFileResponse().getKeyInfo().hasEcReplicationConfig()) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("Key is a key with Erasure Coded replication, which the"
              + " client can not understand."
              + " Please upgrade the client before trying to read the key info"
              + " for "
              + req.getLookupFileRequest().getKeyArgs().getVolumeName()
              + "/" + req.getLookupFileRequest().getKeyArgs().getBucketName()
              + "/" + req.getLookupFileRequest().getKeyArgs().getKeyName()
              + ".")
          .clearLookupFileResponse()
          .build();
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.LookupFile
  )
  public static OMResponse disallowLookupFileWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasLookupFileResponse()) {
      return resp;
    }
    KeyInfo keyInfo = resp.getLookupFileResponse().getKeyInfo();
    if (keyInfo.hasVolumeName() && keyInfo.hasBucketName() &&
        !ctx.getBucketLayout(keyInfo.getVolumeName(), keyInfo.getBucketName())
            .equals(BucketLayout.LEGACY)) {
      resp = resp.toBuilder()
          .setStatus(Status.NOT_SUPPORTED_OPERATION)
          .setMessage("File is present inside a bucket with bucket layout " +
              "features, which the client can not understand. Please upgrade" +
              " the client to a compatible version before trying to read the" +
              " key info for "
              + req.getLookupFileRequest().getKeyArgs().getVolumeName()
              + "/" + req.getLookupFileRequest().getKeyArgs().getBucketName()
              + "/" + req.getLookupFileRequest().getKeyArgs().getKeyName()
              + ".")
          .clearLookupFileResponse()
          .build();
    }
    return resp;
  }

  private ListStatusResponse listStatus(
      ListStatusRequest request, int clientVersion) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setLatestVersionLocation(keyArgs.getLatestVersionLocation())
        .setHeadOp(keyArgs.getHeadOp())
        .build();
    boolean allowPartialPrefixes =
        request.hasAllowPartialPrefix() && request.getAllowPartialPrefix();
    List<OzoneFileStatus> statuses =
        impl.listStatus(omKeyArgs, request.getRecursive(),
            request.getStartKey(), limitListSize(request.getNumEntries()),
            allowPartialPrefixes);
    ListStatusResponse.Builder
        listStatusResponseBuilder =
        ListStatusResponse.newBuilder();
    for (OzoneFileStatus status : statuses) {
      listStatusResponseBuilder.addStatuses(status.getProtobuf(clientVersion));
    }
    return listStatusResponseBuilder.build();
  }

  private ListStatusLightResponse listStatusLight(
      ListStatusRequest request, int clientVersion) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setSortDatanodesInPipeline(false)
        .setLatestVersionLocation(true)
        .setHeadOp(keyArgs.getHeadOp())
        .build();
    boolean allowPartialPrefixes =
        request.hasAllowPartialPrefix() && request.getAllowPartialPrefix();
    List<OzoneFileStatusLight> statuses =
        impl.listStatusLight(omKeyArgs, request.getRecursive(),
            request.getStartKey(), limitListSize(request.getNumEntries()),
            allowPartialPrefixes);
    ListStatusLightResponse.Builder
        listStatusLightResponseBuilder =
        ListStatusLightResponse.newBuilder();
    for (OzoneFileStatusLight status : statuses) {
      listStatusLightResponseBuilder.addStatuses(status.getProtobuf());
    }
    return listStatusLightResponseBuilder.build();
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListStatus
  )
  public static OMResponse disallowListStatusResponseWithECReplicationConfig(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException {
    if (!resp.hasListStatusResponse()) {
      return resp;
    }
    List<OzoneFileStatusProto> statuses =
        resp.getListStatusResponse().getStatusesList();
    for (OzoneFileStatusProto status : statuses) {
      if (status.getKeyInfo().hasEcReplicationConfig()) {
        resp = resp.toBuilder()
            .setStatus(Status.NOT_SUPPORTED_OPERATION)
            .setMessage("The list of keys contains keys with Erasure Coded"
                + " replication set, hence the client is not able to"
                + " represent all the keys returned."
                + " Please upgrade the client to get the list of keys.")
            .clearListStatusResponse()
            .build();
      }
    }
    return resp;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.POST_PROCESS,
      requestType = Type.ListStatus
  )
  public static OMResponse disallowListStatusResponseWithBucketLayout(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws ServiceException, IOException {
    if (!resp.hasListStatusResponse()) {
      return resp;
    }

    // Add the volume and bucket pairs to a set to avoid duplicate entries.
    List<OzoneFileStatusProto> statuses =
        resp.getListStatusResponse().getStatusesList();
    HashSet<Pair<String, String>> volumeBucketSet = new HashSet<>();

    for (OzoneFileStatusProto status : statuses) {
      KeyInfo keyInfo = status.getKeyInfo();
      if (keyInfo.hasVolumeName() && keyInfo.hasBucketName()) {
        volumeBucketSet.add(
            new ImmutablePair<>(keyInfo.getVolumeName(),
                keyInfo.getBucketName()));
      }
    }

    // If any of the keys are present in a bucket with a non LEGACY bucket
    // layout, then the client needs to be upgraded before proceeding.
    for (Pair<String, String> volumeBucket : volumeBucketSet) {
      if (!ctx.getBucketLayout(volumeBucket.getLeft(),
          volumeBucket.getRight()).isLegacy()) {
        resp = resp.toBuilder()
            .setStatus(Status.NOT_SUPPORTED_OPERATION)
            .setMessage("The list of keys is present in a bucket using bucket"
                + " layout features, hence the client is not able to"
                + " represent all the keys returned."
                + " Please upgrade the client to get the list of keys.")
            .clearListStatusResponse()
            .build();
        break;
      }
    }

    return resp;
  }

  private FinalizeUpgradeProgressResponse reportUpgradeProgress(
      FinalizeUpgradeProgressRequest request) throws IOException {
    String upgradeClientId = request.getUpgradeClientId();
    boolean takeover = request.getTakeover();
    boolean readonly = request.getReadonly();

    StatusAndMessages progress =
        impl.queryUpgradeFinalizationProgress(upgradeClientId, takeover,
            readonly);

    UpgradeFinalizationStatus.Status protoStatus =
        UpgradeFinalizationStatus.Status.valueOf(progress.status().name());

    UpgradeFinalizationStatus response =
        UpgradeFinalizationStatus.newBuilder()
            .setStatus(protoStatus)
            .addAllMessages(progress.msgs())
            .build();

    return FinalizeUpgradeProgressResponse.newBuilder()
        .setStatus(response)
        .build();
  }

  private PrepareStatusResponse getPrepareStatus() {
    OzoneManagerPrepareState.State prepareState =
        impl.getPrepareState().getState();
    return PrepareStatusResponse.newBuilder()
        .setStatus(prepareState.getStatus())
        .setCurrentTxnIndex(prepareState.getIndex()).build();
  }

  private GetLifecycleConfigurationResponse infoLifecycleConfiguration(
      GetLifecycleConfigurationRequest request) throws IOException {

    GetLifecycleConfigurationResponse.Builder resp =
        GetLifecycleConfigurationResponse.newBuilder();

    String volume = request.getVolumeName();
    String bucket = request.getBucketName();

    OmLifecycleConfiguration omLifecycleConfiguration =
        impl.getLifecycleConfiguration(volume, bucket);

    resp.setLifecycleConfiguration(omLifecycleConfiguration.getProtobuf());

    return resp.build();
  }

  private GetS3VolumeContextResponse getS3VolumeContext()
      throws IOException {
    return impl.getS3VolumeContext().getProtobuf();
  }

  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  private SnapshotDiffResponse snapshotDiff(
      SnapshotDiffRequest snapshotDiffRequest) throws IOException {
    org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse response =
        impl.snapshotDiff(
            snapshotDiffRequest.getVolumeName(),
            snapshotDiffRequest.getBucketName(),
            snapshotDiffRequest.getFromSnapshot(),
            snapshotDiffRequest.getToSnapshot(),
            snapshotDiffRequest.getToken(),
            snapshotDiffRequest.getPageSize(),
            snapshotDiffRequest.getForceFullDiff(),
            snapshotDiffRequest.getDisableNativeDiff());

    SnapshotDiffResponse.Builder builder = SnapshotDiffResponse.newBuilder()
        .setJobStatus(response.getJobStatus().toProtobuf())
        .setWaitTimeInMs(response.getWaitTimeInMs());

    if (StringUtils.isNotEmpty(response.getReason())) {
      builder.setReason(response.getReason());
    }
    if (response.getSnapshotDiffReport() != null) {
      builder.setSnapshotDiffReport(
          response.getSnapshotDiffReport().toProtobuf());
    }

    return builder.build();
  }

  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  private CancelSnapshotDiffResponse cancelSnapshotDiff(
      CancelSnapshotDiffRequest cancelSnapshotDiffRequest) throws IOException {

    org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse response =
        impl.cancelSnapshotDiff(
            cancelSnapshotDiffRequest.getVolumeName(),
            cancelSnapshotDiffRequest.getBucketName(),
            cancelSnapshotDiffRequest.getFromSnapshot(),
            cancelSnapshotDiffRequest.getToSnapshot());

    CancelSnapshotDiffResponse.Builder builder = CancelSnapshotDiffResponse
        .newBuilder();

    if (StringUtils.isNotEmpty(response.getMessage())) {
      builder.setReason(response.getMessage());
    }

    return builder.build();
  }

  private ListSnapshotDiffJobResponse listSnapshotDiffJobs(
      ListSnapshotDiffJobRequest listSnapshotDiffJobRequest
  ) throws IOException {
    String prevSnapshotDiffJob = listSnapshotDiffJobRequest.hasPrevSnapshotDiffJob() ?
        listSnapshotDiffJobRequest.getPrevSnapshotDiffJob() : null;
    int maxListResult = listSnapshotDiffJobRequest.hasMaxListResult() ?
        listSnapshotDiffJobRequest.getMaxListResult() : impl.getOmSnapshotManager().getMaxPageSize();

    org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse response = impl.listSnapshotDiffJobs(
        listSnapshotDiffJobRequest.getVolumeName(),
        listSnapshotDiffJobRequest.getBucketName(),
        listSnapshotDiffJobRequest.getJobStatus(),
        listSnapshotDiffJobRequest.getListAll(),
        prevSnapshotDiffJob,
        maxListResult);

    ListSnapshotDiffJobResponse.Builder builder = ListSnapshotDiffJobResponse.newBuilder();

    for (SnapshotDiffJob diffJob : response.getSnapshotDiffJobs()) {
      builder.addSnapshotDiffJob(diffJob.toProtoBuf());
    }

    if (StringUtils.isNotEmpty(response.getLastSnapshotDiffJob())) {
      builder.setLastSnapshotDiffJob(response.getLastSnapshotDiffJob());
    }

    return builder.build();
  }

  private PrintCompactionLogDagResponse printCompactionLogDag(
      PrintCompactionLogDagRequest printCompactionLogDagRequest)
      throws IOException {
    String message = impl.printCompactionLogDag(
        printCompactionLogDagRequest.getFileNamePrefix(),
        printCompactionLogDagRequest.getGraphType());
    return PrintCompactionLogDagResponse.newBuilder()
        .setMessage(message)
        .build();
  }

  public OzoneManager getOzoneManager() {
    return impl;
  }

  private EchoRPCResponse echoRPC(EchoRPCRequest req) {
    EchoRPCResponse.Builder builder = EchoRPCResponse.newBuilder();
    final ByteString payloadBytes = PayloadUtils.generatePayloadProto2(req.getPayloadSizeResp());
    builder.setPayload(payloadBytes);
    return builder.build();
  }

  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  private OzoneManagerProtocolProtos.SnapshotInfoResponse getSnapshotInfo(
      OzoneManagerProtocolProtos.SnapshotInfoRequest request)
      throws IOException {
    SnapshotInfo snapshotInfo = impl.getSnapshotInfo(request.getVolumeName(),
        request.getBucketName(), request.getSnapshotName());

    return OzoneManagerProtocolProtos.SnapshotInfoResponse.newBuilder()
        .setSnapshotInfo(snapshotInfo.getProtobuf()).build();
  }

  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  private OzoneManagerProtocolProtos.ListSnapshotResponse getSnapshots(
      OzoneManagerProtocolProtos.ListSnapshotRequest request)
      throws IOException {
    ListSnapshotResponse implResponse = impl.listSnapshot(
        request.getVolumeName(), request.getBucketName(), request.getPrefix(),
        request.getPrevSnapshot(), limitListSizeInt(request.getMaxListResult()));

    List<OzoneManagerProtocolProtos.SnapshotInfo> snapshotInfoList = implResponse.getSnapshotInfos()
        .stream().map(SnapshotInfo::getProtobuf).collect(Collectors.toList());

    OzoneManagerProtocolProtos.ListSnapshotResponse.Builder builder =
        OzoneManagerProtocolProtos.ListSnapshotResponse.newBuilder().addAllSnapshotInfo(snapshotInfoList);
    if (StringUtils.isNotEmpty(implResponse.getLastSnapshot())) {
      builder.setLastSnapshot(implResponse.getLastSnapshot());
    }
    return builder.build();
  }

  private TransferLeadershipResponseProto transferLeadership(
      TransferLeadershipRequestProto req) throws IOException {
    String newLeaderId = req.getNewLeaderId();
    impl.transferLeadership(newLeaderId);
    return TransferLeadershipResponseProto.getDefaultInstance();
  }

  private SetSafeModeResponse setSafeMode(
      SetSafeModeRequest req) throws IOException {
    OzoneManagerProtocolProtos.SafeMode safeMode = req.getSafeMode();
    boolean response = impl.setSafeMode(toSafeModeAction(safeMode), false);
    return SetSafeModeResponse.newBuilder()
        .setResponse(response)
        .build();
  }

  private GetObjectTaggingResponse getObjectTagging(GetObjectTaggingRequest request)
      throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();

    GetObjectTaggingResponse.Builder resp =
        GetObjectTaggingResponse.newBuilder();

    Map<String, String> result = impl.getObjectTagging(omKeyArgs);

    resp.addAllTags(KeyValueUtil.toProtobuf(result));
    return resp.build();
  }

  private SafeModeAction toSafeModeAction(
      OzoneManagerProtocolProtos.SafeMode safeMode) {
    switch (safeMode) {
    case ENTER:
      return SafeModeAction.ENTER;
    case LEAVE:
      return SafeModeAction.LEAVE;
    case FORCE_EXIT:
      return SafeModeAction.FORCE_EXIT;
    case GET:
      return SafeModeAction.GET;
    default:
      throw new IllegalArgumentException("Unexpected safe mode action " +
          safeMode);
    }
  }

  private OzoneManagerProtocolProtos.GetQuotaRepairStatusResponse getQuotaRepairStatus(
      OzoneManagerProtocolProtos.GetQuotaRepairStatusRequest req) throws IOException {
    return OzoneManagerProtocolProtos.GetQuotaRepairStatusResponse.newBuilder()
        .setStatus(impl.getQuotaRepairStatus())
        .build();
  }

  private OzoneManagerProtocolProtos.StartQuotaRepairResponse startQuotaRepair(
      OzoneManagerProtocolProtos.StartQuotaRepairRequest req) throws IOException {
    impl.startQuotaRepair(req.getBucketsList());
    return OzoneManagerProtocolProtos.StartQuotaRepairResponse.newBuilder().build();
  }

  private int limitListSizeInt(int requestedSize) {
    return Math.toIntExact(limitListSize(requestedSize));
  }

  private long limitListSize(long requestedSize) {
    return Math.min(requestedSize, impl.getConfig().getMaxListSize());
  }

}
