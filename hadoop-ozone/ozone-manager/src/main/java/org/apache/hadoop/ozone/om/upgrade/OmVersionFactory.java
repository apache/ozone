package org.apache.hadoop.ozone.om.upgrade;

import java.util.function.Function;

import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequestV2;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMTrashRecoverRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3GetSecretRequest;
import org.apache.hadoop.ozone.om.request.security.OMCancelDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMGetDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeDeleteRequest;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.apache.hadoop.ozone.upgrade.VersionedInstanceSupplierFactory;

/**
 * Wrapper class for OM around a generic Versioned Instance factory.
 */
public class OmVersionFactory {

  public OmVersionFactory() {
    registerClientRequests();
  }

  private VersionedInstanceSupplierFactory<OMRequest, OMClientRequest>
      writeRequestFactory = new VersionedInstanceSupplierFactory<>();

  public void registerClientRequest(String type,
                                      Function<OMRequest, OMClientRequest>
                                          function) {
    writeRequestFactory.register(
        new VersionFactoryKey.Builder().key(type).build(), function);
  }

  /**
   * New client requests go here.
   */
  private void registerClientRequests() {
    registerClientRequest(Type.CreateVolume.name(),
        OMVolumeCreateRequest::new);
    registerClientRequest(Type.SetVolumeProperty.name(),
        OzoneManagerRatisUtils::getVolumeSetOwnerRequest);
    registerClientRequest(Type.DeleteVolume.name(), OMVolumeDeleteRequest::new);
    registerClientRequest(Type.CreateBucket.name(), OMBucketCreateRequest::new);
    registerClientRequest(Type.DeleteBucket.name(), OMBucketDeleteRequest::new);
    registerClientRequest(Type.SetBucketProperty.name(),
        OMBucketSetPropertyRequest::new);
    registerClientRequest(Type.AllocateBlock.name(),
        OMAllocateBlockRequest::new);
    registerClientRequest(Type.CreateKey.name(), OMKeyCreateRequest::new);
    registerClientRequest(Type.CommitKey.name(), OMKeyCommitRequest::new);
    registerClientRequest(Type.DeleteKey.name(), OMKeyDeleteRequest::new);
    registerClientRequest(Type.DeleteKeys.name(), OMKeysDeleteRequest::new);
    registerClientRequest(Type.RenameKey.name(), OMKeyRenameRequest::new);
    registerClientRequest(Type.CreateDirectory.name(),
        OMDirectoryCreateRequest::new);
    registerClientRequest(Type.CreateFile.name(), OMFileCreateRequest::new);
    registerClientRequest(Type.PurgeKeys.name(), OMKeyPurgeRequest::new);
    registerClientRequest(Type.InitiateMultiPartUpload.name(),
        S3InitiateMultipartUploadRequest::new);
    registerClientRequest(Type.CommitMultiPartUpload.name(),
        S3MultipartUploadCommitPartRequest::new);
    registerClientRequest(Type.AbortMultiPartUpload.name(),
        S3MultipartUploadAbortRequest::new);
    registerClientRequest(Type.CompleteMultiPartUpload.name(),
        S3MultipartUploadCompleteRequest::new);
    registerClientRequest(Type.AddAcl.name(),
        OzoneManagerRatisUtils::getOMAclRequest);
    registerClientRequest(Type.RemoveAcl.name(),
        OzoneManagerRatisUtils::getOMAclRequest);
    registerClientRequest(Type.SetAcl.name(),
        OzoneManagerRatisUtils::getOMAclRequest);
    registerClientRequest(Type.GetDelegationToken.name(),
        OMGetDelegationTokenRequest::new);
    registerClientRequest(Type.CancelDelegationToken.name(),
        OMCancelDelegationTokenRequest::new);
    registerClientRequest(Type.RenewDelegationToken.name(),
        OMRenewDelegationTokenRequest::new);
    registerClientRequest(Type.GetS3Secret.name(), S3GetSecretRequest::new);
    registerClientRequest(Type.RecoverTrash.name(), OMTrashRecoverRequest::new);

    // Adding "Mock" Create Key Request V2.
    registerClientRequest(Type.CreateKey.name(), OMLayoutFeature.NEW_FEATURE,
        OMKeyCreateRequestV2::new);
  }

  public void registerClientRequest(
      String type, OMLayoutFeature layoutFeature,
      Function<OMRequest, OMClientRequest> function) {
    VersionFactoryKey key = new VersionFactoryKey.Builder()
        .key(type).version(layoutFeature.layoutVersion()).build();
    writeRequestFactory.register(key, function);
  }

  public OMClientRequest getClientRequest(OMRequest omRequest) {
    Function<OMRequest, ? extends OMClientRequest> instance =
        writeRequestFactory.getInstance(getVersionFactoryKey(omRequest));
    return instance.apply(omRequest);
  }

  private VersionFactoryKey getVersionFactoryKey(OMRequest omRequest) {
    return new VersionFactoryKey.Builder()
        .version(Math.toIntExact(omRequest.getLayoutVersion().getVersion()))
        .key(omRequest.getCmdType().name())
        .build();
  }

  public VersionedInstanceSupplierFactory<OMRequest, OMClientRequest>
      getRequestFactory() {
    return writeRequestFactory;
  }
}
