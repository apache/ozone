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

package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.SequenceNumberNotFoundException;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTrashResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverTrashResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

import com.google.common.collect.Lists;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadInfo;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartInfo;
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
  private OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  public OzoneManagerRequestHandler(OzoneManager om,
      OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer) {
    this.impl = om;
    this.ozoneManagerDoubleBuffer = ozoneManagerDoubleBuffer;
  }

  //TODO simplify it to make it shorter
  @SuppressWarnings("methodlength")
  @Override
  public OMResponse handleReadRequest(OMRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received OMRequest: {}, ", request);
    }
    Type cmdType = request.getCmdType();
    OMResponse.Builder responseBuilder = OMResponse.newBuilder()
        .setCmdType(cmdType)
        .setStatus(Status.OK);
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
            request.getLookupKeyRequest());
        responseBuilder.setLookupKeyResponse(lookupKeyResponse);
        break;
      case ListKeys:
        ListKeysResponse listKeysResponse = listKeys(
            request.getListKeysRequest());
        responseBuilder.setListKeysResponse(listKeysResponse);
        break;
      case ListTrash:
        ListTrashResponse listTrashResponse = listTrash(
            request.getListTrashRequest());
        responseBuilder.setListTrashResponse(listTrashResponse);
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
      case ServiceList:
        ServiceListResponse serviceListResponse = getServiceList(
            request.getServiceListRequest());
        responseBuilder.setServiceListResponse(serviceListResponse);
        break;
      case DBUpdates:
        DBUpdatesResponse dbUpdatesResponse = getOMDBUpdates(
            request.getDbUpdatesRequest());
        responseBuilder.setDbUpdatesResponse(dbUpdatesResponse);
        break;
      case GetFileStatus:
        GetFileStatusResponse getFileStatusResponse =
            getOzoneFileStatus(request.getGetFileStatusRequest());
        responseBuilder.setGetFileStatusResponse(getFileStatusResponse);
        break;
      case LookupFile:
        LookupFileResponse lookupFileResponse =
            lookupFile(request.getLookupFileRequest());
        responseBuilder.setLookupFileResponse(lookupFileResponse);
        break;
      case ListStatus:
        ListStatusResponse listStatusResponse =
            listStatus(request.getListStatusRequest());
        responseBuilder.setListStatusResponse(listStatusResponse);
        break;
      case GetAcl:
        GetAclResponse getAclResponse =
            getAcl(request.getGetAclRequest());
        responseBuilder.setGetAclResponse(getAclResponse);
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
  public OMClientResponse handleWriteRequest(OMRequest omRequest,
      long transactionLogIndex) {
    OMClientRequest omClientRequest =
        OzoneManagerRatisUtils.createClientRequest(omRequest);
    OMClientResponse omClientResponse =
        omClientRequest.validateAndUpdateCache(getOzoneManager(),
            transactionLogIndex, ozoneManagerDoubleBuffer::add);
    return omClientResponse;
  }

  @Override
  public void updateDoubleBuffer(OzoneManagerDoubleBuffer omDoubleBuffer) {
    this.ozoneManagerDoubleBuffer = omDoubleBuffer;
  }

  private DBUpdatesResponse getOMDBUpdates(
      DBUpdatesRequest dbUpdatesRequest)
      throws SequenceNumberNotFoundException {

    DBUpdatesResponse.Builder builder = DBUpdatesResponse
        .newBuilder();
    DBUpdates dbUpdatesWrapper =
        impl.getDBUpdates(dbUpdatesRequest);
    for (int i = 0; i < dbUpdatesWrapper.getData().size(); i++) {
      builder.addData(OMPBHelper.getByteString(
          dbUpdatesWrapper.getData().get(i)));
    }
    builder.setSequenceNumber(dbUpdatesWrapper.getCurrentSequenceNumber());
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

  private LookupKeyResponse lookupKey(LookupKeyRequest request)
      throws IOException {
    LookupKeyResponse.Builder resp =
        LookupKeyResponse.newBuilder();
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(keyArgs.getSortDatanodes())
        .build();
    OmKeyInfo keyInfo = impl.lookupKey(omKeyArgs);
    resp.setKeyInfo(keyInfo.getProtobuf());

    return resp.build();
  }

  private ListBucketsResponse listBuckets(ListBucketsRequest request)
      throws IOException {
    ListBucketsResponse.Builder resp =
        ListBucketsResponse.newBuilder();

    List<OmBucketInfo> buckets = impl.listBuckets(
        request.getVolumeName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount());
    for (OmBucketInfo bucket : buckets) {
      resp.addBucketInfo(bucket.getProtobuf());
    }

    return resp.build();
  }

  private ListKeysResponse listKeys(ListKeysRequest request)
      throws IOException {
    ListKeysResponse.Builder resp =
        ListKeysResponse.newBuilder();

    List<OmKeyInfo> keys = impl.listKeys(
        request.getVolumeName(),
        request.getBucketName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount());
    for (OmKeyInfo key : keys) {
      resp.addKeyInfo(key.getProtobuf());
    }

    return resp.build();
  }

  private ListTrashResponse listTrash(ListTrashRequest request)
      throws IOException {

    ListTrashResponse.Builder resp =
        ListTrashResponse.newBuilder();

    List<RepeatedOmKeyInfo> deletedKeys = impl.listTrash(
        request.getVolumeName(),
        request.getBucketName(),
        request.getStartKeyName(),
        request.getKeyPrefix(),
        request.getMaxKeys());

    for (RepeatedOmKeyInfo key: deletedKeys) {
      resp.addDeletedKeys(key.getProto());
    }

    return resp.build();
  }

  private RecoverTrashResponse recoverTrash(RecoverTrashRequest request)
      throws IOException {

    RecoverTrashResponse.Builder resp =
        RecoverTrashResponse.newBuilder();

    boolean recoverKeys = impl.recoverTrash(
        request.getVolumeName(),
        request.getBucketName(),
        request.getKeyName(),
        request.getDestinationBucket());

    return resp.setResponse(recoverKeys).build();
  }

  private AllocateBlockResponse allocateBlock(AllocateBlockRequest request)
      throws IOException {
    AllocateBlockResponse.Builder resp =
        AllocateBlockResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();

    OmKeyLocationInfo newLocation = impl.allocateBlock(omKeyArgs,
        request.getClientID(), ExcludeList.getFromProtoBuf(
            request.getExcludeList()));

    resp.setKeyLocation(newLocation.getProtobuf());

    return resp.build();
  }

  private ServiceListResponse getServiceList(ServiceListRequest request)
      throws IOException {
    ServiceListResponse.Builder resp = ServiceListResponse.newBuilder();

    ServiceInfoEx serviceInfoEx = impl.getServiceInfo();
    resp.addAllServiceInfo(serviceInfoEx.getServiceInfoList().stream()
        .map(ServiceInfo::getProtobuf)
        .collect(Collectors.toList()));
    if (serviceInfoEx.getCaCertificate() != null) {
      resp.setCaCertificate(serviceInfoEx.getCaCertificate());
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

    response.setType(omMultipartUploadListParts.getReplicationType());
    response.setFactor(omMultipartUploadListParts.getReplicationFactor());
    response.setNextPartNumberMarker(
        omMultipartUploadListParts.getNextPartNumberMarker());
    response.setIsTruncated(omMultipartUploadListParts.isTruncated());

    return response.addAllPartsList(partInfoList).build();


  }

  private ListMultipartUploadsResponse listMultipartUploads(
      ListMultipartUploadsRequest request)
      throws IOException {

    OmMultipartUploadList omMultipartUploadList =
        impl.listMultipartUploads(request.getVolume(), request.getBucket(),
            request.getPrefix());

    List<MultipartUploadInfo> info = omMultipartUploadList
        .getUploads()
        .stream()
        .map(upload -> MultipartUploadInfo.newBuilder()
            .setVolumeName(upload.getVolumeName())
            .setBucketName(upload.getBucketName())
            .setKeyName(upload.getKeyName())
            .setUploadId(upload.getUploadId())
            .setType(upload.getReplicationType())
            .setFactor(upload.getReplicationFactor())
            .setCreationTime(upload.getCreationTime().toEpochMilli())
            .build())
        .collect(Collectors.toList());

    ListMultipartUploadsResponse response =
        ListMultipartUploadsResponse.newBuilder()
            .addAllUploadsList(info)
            .build();

    return response;
  }

  private GetFileStatusResponse getOzoneFileStatus(
      GetFileStatusRequest request) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .build();

    GetFileStatusResponse.Builder rb = GetFileStatusResponse.newBuilder();
    rb.setStatus(impl.getFileStatus(omKeyArgs).getProtobuf());

    return rb.build();
  }

  private LookupFileResponse lookupFile(
      LookupFileRequest request)
      throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(keyArgs.getSortDatanodes())
        .build();
    return LookupFileResponse.newBuilder()
        .setKeyInfo(impl.lookupFile(omKeyArgs).getProtobuf())
        .build();
  }

  private ListStatusResponse listStatus(
      ListStatusRequest request) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .build();
    List<OzoneFileStatus> statuses =
        impl.listStatus(omKeyArgs, request.getRecursive(),
            request.getStartKey(), request.getNumEntries());
    ListStatusResponse.Builder
        listStatusResponseBuilder =
        ListStatusResponse.newBuilder();
    for (OzoneFileStatus status : statuses) {
      listStatusResponseBuilder.addStatuses(status.getProtobuf());
    }
    return listStatusResponseBuilder.build();
  }

  protected OzoneManager getOzoneManager() {
    return impl;
  }
}
