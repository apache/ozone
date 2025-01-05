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

package org.apache.hadoop.ozone.om.lock;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketAddAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketSetAclRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeySetTimesRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysRenameRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeDeleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeAddAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeSetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTimesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for obs request for lock.
 */
public class TestRequestLock {
  private OzoneManager ozoneManager;

  @Test
  public void testObsRequestLockUnlock() throws IOException {
    ozoneManager = mock(OzoneManager.class);
    ResolvedBucket resolvedBucket = new ResolvedBucket("testvol", "testbucket",
        "testvol", "testbucket", "none", BucketLayout.OBJECT_STORE);
    when(ozoneManager.resolveBucketLink(Mockito.any(Pair.class), Mockito.anyBoolean(), Mockito.anyBoolean()))
        .thenReturn(resolvedBucket);
    OMMetadataManager mockMetaManager = mock(OMMetadataManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(mockMetaManager);
    when(mockMetaManager.getBucketKey(anyString(), anyString())).thenReturn("bucketkey");
    Table<String, OmBucketInfo> mockTable = mock(Table.class);
    when(mockMetaManager.getBucketTable()).thenReturn(mockTable);
    when(mockTable.get(anyString())).thenReturn(OmBucketInfo.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVolumeName("testvol").setBucketName("testbucket").setAcls(Collections.EMPTY_LIST)
        .setStorageType(StorageType.SSD).build());

    OMRequest omRequest;
    OmLockOpr omLockOpr = new OmLockOpr("test-");
    KeyArgs keyArgs = KeyArgs.newBuilder().setVolumeName("testvol").setBucketName("testbucket").setKeyName("testkey")
        .build();

    CreateDirectoryRequest dirReq = CreateDirectoryRequest.newBuilder().setKeyArgs(keyArgs).build();
    omRequest = getReqBuilder(Type.CreateDirectory).setCreateDirectoryRequest(dirReq).build();
    validateLockUnlock(new OMDirectoryCreateRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 1);

    CreateFileRequest fileReq = CreateFileRequest.newBuilder().setKeyArgs(keyArgs).setIsRecursive(true)
        .setIsOverwrite(true).build();
    omRequest = getReqBuilder(Type.CreateFile).setCreateFileRequest(fileReq).build();
    validateLockUnlock(new OMFileCreateRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 1);

    AllocateBlockRequest blockReq = AllocateBlockRequest.newBuilder().setKeyArgs(keyArgs).setClientID(123).build();
    omRequest = getReqBuilder(Type.AllocateBlock).setAllocateBlockRequest(blockReq).build();
    validateLockUnlock(new OMAllocateBlockRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    CommitKeyRequest commitReq = CommitKeyRequest.newBuilder().setKeyArgs(keyArgs).setClientID(123).build();
    omRequest = getReqBuilder(Type.CommitKey).setCommitKeyRequest(commitReq).build();
    validateLockUnlock(new OMKeyCommitRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    CreateKeyRequest createKeyRequest = CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();
    omRequest = getReqBuilder(Type.CreateKey).setCreateKeyRequest(createKeyRequest).build();
    validateLockUnlock(new OMKeyCreateRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 1);

    DeleteKeyRequest deleteKeyRequest = DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();
    omRequest = getReqBuilder(Type.DeleteKey).setDeleteKeyRequest(deleteKeyRequest).build();
    validateLockUnlock(new OMKeyDeleteRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder().setKeyArgs(keyArgs).setToKeyName("key2").build();
    omRequest = getReqBuilder(Type.RenameKey).setRenameKeyRequest(renameKeyRequest).build();
    validateLockUnlock(new OMKeyRenameRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 3);

    OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeysArgs = OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
        .addKeys("test1").addKeys("test2").setVolumeName("testvol").setBucketName("testbucket").build();
    DeleteKeysRequest deleteKeysRequest = DeleteKeysRequest.newBuilder().setDeleteKeys(deleteKeysArgs).build();
    omRequest = getReqBuilder(Type.DeleteKeys).setDeleteKeysRequest(deleteKeysRequest).build();
    validateLockUnlock(new OMKeysDeleteRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 3);

    SetTimesRequest setTimesRequest = SetTimesRequest.newBuilder().setKeyArgs(keyArgs).setMtime(123).setAtime(123)
        .build();
    omRequest = getReqBuilder(Type.SetTimes).setSetTimesRequest(setTimesRequest).build();
    validateLockUnlock(new OMKeySetTimesRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    OzoneManagerProtocolProtos.RenameKeysMap renameKeysMap = OzoneManagerProtocolProtos.RenameKeysMap.newBuilder()
        .setFromKeyName("test2").setToKeyName("test1").build();
    OzoneManagerProtocolProtos.RenameKeysArgs renameKeyArgs = OzoneManagerProtocolProtos.RenameKeysArgs.newBuilder()
        .addRenameKeysMap(renameKeysMap).setVolumeName("testvol").setBucketName("testbucket").build();
    RenameKeysRequest renameKeysRequest = RenameKeysRequest.newBuilder().setRenameKeysArgs(renameKeyArgs).build();
    omRequest = getReqBuilder(Type.RenameKeys).setRenameKeysRequest(renameKeysRequest).build();
    validateLockUnlock(new OMKeysRenameRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 3);

    OzoneManagerProtocolProtos.OzoneObj objArg = OzoneManagerProtocolProtos.OzoneObj.newBuilder()
        .setResType(OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE).setPath("/k/b/abc.txtß").build();
    OzoneManagerProtocolProtos.OzoneAclInfo aclObj = OzoneManagerProtocolProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName("testuser").setRights(ByteString.copyFromUtf8("*"))
        .setAclScope(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.DEFAULT)
        .build();
    AddAclRequest aclAddRequest = AddAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.AddAcl).setAddAclRequest(aclAddRequest).build();
    validateLockUnlock(new OMKeyAddAclRequest(omRequest, ozoneManager), omLockOpr, 2);

    RemoveAclRequest removeAclRequest = RemoveAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.RemoveAcl).setRemoveAclRequest(removeAclRequest).build();
    validateLockUnlock(new OMKeyRemoveAclRequest(omRequest, ozoneManager), omLockOpr, 2);

    SetAclRequest setAclRequest = SetAclRequest.newBuilder().setObj(objArg).build();
    omRequest = getReqBuilder(Type.SetAcl).setSetAclRequest(setAclRequest).build();
    validateLockUnlock(new OMKeySetAclRequest(omRequest, ozoneManager), omLockOpr, 2);

    MultipartInfoInitiateRequest initiateReq = MultipartInfoInitiateRequest.newBuilder().setKeyArgs(keyArgs).build();
    omRequest = getReqBuilder(Type.InitiateMultiPartUpload).setInitiateMultiPartUploadRequest(initiateReq).build();
    validateLockUnlock(new S3InitiateMultipartUploadRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 1);

    MultipartUploadAbortRequest abortReq = MultipartUploadAbortRequest.newBuilder().setKeyArgs(keyArgs).build();
    omRequest = getReqBuilder(Type.AbortMultiPartUpload).setAbortMultiPartUploadRequest(abortReq).build();
    validateLockUnlock(new S3MultipartUploadAbortRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    MultipartCommitUploadPartRequest multipartCommitReq = MultipartCommitUploadPartRequest.newBuilder()
        .setKeyArgs(keyArgs).setClientID(123).build();
    omRequest = getReqBuilder(Type.CommitMultiPartUpload).setCommitMultiPartUploadRequest(multipartCommitReq).build();
    validateLockUnlock(new S3MultipartUploadCommitPartRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);

    MultipartUploadCompleteRequest completeRequest = MultipartUploadCompleteRequest.newBuilder().setKeyArgs(keyArgs)
        .build();
    omRequest = getReqBuilder(Type.CompleteMultiPartUpload).setCompleteMultiPartUploadRequest(completeRequest).build();
    validateLockUnlock(new S3MultipartUploadCompleteRequest(omRequest, BucketLayout.OBJECT_STORE), omLockOpr, 2);
  }

  @Test
  public void testBucketRequestLockUnlock() throws IOException {
    ozoneManager = mock(OzoneManager.class);
    ResolvedBucket resolvedBucket = new ResolvedBucket("testvol", "testbucket",
        "testvol", "testbucket", "none", BucketLayout.OBJECT_STORE);
    when(ozoneManager.resolveBucketLink(Mockito.any(Pair.class), Mockito.anyBoolean(), Mockito.anyBoolean()))
        .thenReturn(resolvedBucket);

    OMRequest omRequest;
    OmLockOpr omLockOpr = new OmLockOpr("test-");

    OzoneManagerProtocolProtos.BucketInfo bucketInfo = OzoneManagerProtocolProtos.BucketInfo.newBuilder()
        .setVolumeName("testvol").setBucketName("testbucket").setIsVersionEnabled(false)
        .setStorageType(OzoneManagerProtocolProtos.StorageTypeProto.DISK).build();
    CreateBucketRequest createBucketRequest = CreateBucketRequest.newBuilder().setBucketInfo(bucketInfo).build();
    omRequest = getReqBuilder(Type.CreateBucket).setCreateBucketRequest(createBucketRequest).build();
    validateLockUnlock(new OMBucketCreateRequest(omRequest), omLockOpr, 2);

    DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.newBuilder().setBucketName("testbucket")
        .setVolumeName("testvolume").build();
    omRequest = getReqBuilder(Type.DeleteBucket).setDeleteBucketRequest(deleteBucketRequest).build();
    validateLockUnlock(new OMBucketDeleteRequest(omRequest), omLockOpr, 1);

    OzoneManagerProtocolProtos.BucketArgs args = OzoneManagerProtocolProtos.BucketArgs.newBuilder()
        .setVolumeName("testvol").setBucketName("tetbucket").build();
    SetBucketPropertyRequest bucketPropertyRequest = SetBucketPropertyRequest.newBuilder().setBucketArgs(args).build();
    omRequest = getReqBuilder(Type.SetBucketProperty).setSetBucketPropertyRequest(bucketPropertyRequest).build();
    validateLockUnlock(new OMBucketSetOwnerRequest(omRequest), omLockOpr, 1);
    validateLockUnlock(new OMBucketSetPropertyRequest(omRequest), omLockOpr, 1);

    OzoneManagerProtocolProtos.OzoneObj objArg = OzoneManagerProtocolProtos.OzoneObj.newBuilder()
        .setResType(OzoneManagerProtocolProtos.OzoneObj.ObjectType.BUCKET)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE).setPath("/k/b").build();
    OzoneManagerProtocolProtos.OzoneAclInfo aclObj = OzoneManagerProtocolProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName("testuser").setRights(ByteString.copyFromUtf8("*"))
        .setAclScope(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.DEFAULT)
        .build();
    AddAclRequest aclAddRequest = AddAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.AddAcl).setAddAclRequest(aclAddRequest).build();
    validateLockUnlock(new OMBucketAddAclRequest(omRequest), omLockOpr, 2);

    RemoveAclRequest removeAclRequest = RemoveAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.RemoveAcl).setRemoveAclRequest(removeAclRequest).build();
    validateLockUnlock(new OMBucketRemoveAclRequest(omRequest), omLockOpr, 2);

    SetAclRequest setAclRequest = SetAclRequest.newBuilder().setObj(objArg).build();
    omRequest = getReqBuilder(Type.SetAcl).setSetAclRequest(setAclRequest).build();
    validateLockUnlock(new OMBucketSetAclRequest(omRequest), omLockOpr, 2);
  }

  @Test
  public void testVolumeRequestLockUnlock() throws IOException {
    ozoneManager = mock(OzoneManager.class);
    ResolvedBucket resolvedBucket = new ResolvedBucket("testvol", "testbucket",
        "testvol", "testbucket", "none", BucketLayout.OBJECT_STORE);
    when(ozoneManager.resolveBucketLink(Mockito.any(Pair.class), Mockito.anyBoolean(), Mockito.anyBoolean()))
        .thenReturn(resolvedBucket);

    OMRequest omRequest;
    OmLockOpr omLockOpr = new OmLockOpr("test-");

    OzoneManagerProtocolProtos.VolumeInfo volumeInfo = OzoneManagerProtocolProtos.VolumeInfo.newBuilder()
        .setVolume("testvol").setAdminName("admin").setOwnerName("owner").build();
    CreateVolumeRequest createVolumeRequest = CreateVolumeRequest.newBuilder().setVolumeInfo(volumeInfo).build();
    omRequest = getReqBuilder(Type.CreateVolume).setCreateVolumeRequest(createVolumeRequest).build();
    validateLockUnlock(new OMVolumeCreateRequest(omRequest), omLockOpr, 1);

    DeleteVolumeRequest deleteVolumeRequest = DeleteVolumeRequest.newBuilder().setVolumeName("testvolume").build();
    omRequest = getReqBuilder(Type.DeleteVolume).setDeleteVolumeRequest(deleteVolumeRequest).build();
    validateLockUnlock(new OMVolumeDeleteRequest(omRequest), omLockOpr, 1);

    SetVolumePropertyRequest volumePropertyRequest = SetVolumePropertyRequest.newBuilder().setVolumeName("testVolume").build();
    omRequest = getReqBuilder(Type.SetVolumeProperty).setSetVolumePropertyRequest(volumePropertyRequest).build();
    validateLockUnlock(new OMVolumeSetOwnerRequest(omRequest), omLockOpr, 1);
    validateLockUnlock(new OMVolumeSetQuotaRequest(omRequest), omLockOpr, 1);

    OzoneManagerProtocolProtos.OzoneObj objArg = OzoneManagerProtocolProtos.OzoneObj.newBuilder()
        .setResType(OzoneManagerProtocolProtos.OzoneObj.ObjectType.VOLUME)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE).setPath("/k").build();
    OzoneManagerProtocolProtos.OzoneAclInfo aclObj = OzoneManagerProtocolProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName("testuser").setRights(ByteString.copyFromUtf8("*"))
        .setAclScope(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.DEFAULT)
        .build();
    AddAclRequest aclAddRequest = AddAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.AddAcl).setAddAclRequest(aclAddRequest).build();
    validateLockUnlock(new OMVolumeAddAclRequest(omRequest), omLockOpr, 1);

    RemoveAclRequest removeAclRequest = RemoveAclRequest.newBuilder().setObj(objArg).setAcl(aclObj).build();
    omRequest = getReqBuilder(Type.RemoveAcl).setRemoveAclRequest(removeAclRequest).build();
    validateLockUnlock(new OMVolumeRemoveAclRequest(omRequest), omLockOpr, 1);

    SetAclRequest setAclRequest = SetAclRequest.newBuilder().setObj(objArg).build();
    omRequest = getReqBuilder(Type.SetAcl).setSetAclRequest(setAclRequest).build();
    validateLockUnlock(new OMVolumeSetAclRequest(omRequest), omLockOpr, 1);
  }

  private OMRequest.Builder getReqBuilder(Type type) {
    return OMRequest.newBuilder().setClientId("clitest").setCmdType(type);
  }

  private void validateLockUnlock(OMClientRequest clientRequest, OmLockOpr omLockOpr, long lockCnt) throws IOException {
    OmLockOpr.OmLockInfo lockInfo = clientRequest.lock(ozoneManager, omLockOpr);
    assertEquals(lockCnt, lockInfo.getLocks().size());
    clientRequest.unlock(omLockOpr, lockInfo);
    assertEquals(0, omLockOpr.getLockMonitorMap().size());

    OmLockOpr.OmLockInfo lockInfoAgain = clientRequest.lock(ozoneManager, omLockOpr);
    assertEquals(lockCnt, lockInfoAgain.getLocks().size());
    clientRequest.unlock(omLockOpr, lockInfoAgain);
    assertEquals(0, omLockOpr.getLockMonitorMap().size());
  }
}
